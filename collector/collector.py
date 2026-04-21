"""
Live Collector: Binance + Polymarket
=====================================
Собирает данные одновременно с Binance и Polymarket.
Симулирует сделки по сигналам и считает P&L.

Что делает:
  1. Слушает WebSocket Binance — сделки BTC в реальном времени
  2. Каждые 2 секунды опрашивает стакан Binance
  3. Каждые 2 секунды опрашивает цену UP/DOWN на Polymarket
  4. Когда видит кластерный сигнал — записывает "виртуальную ставку"
  5. Через N секунд проверяет результат и считает P&L
  6. Пишет всё в CSV для последующего анализа

Запуск:
  pip install aiohttp websockets pandas
  python live_collector.py

Файлы на выходе:
  live_binance.csv      — все сделки Binance
  live_polymarket.csv   — снимки цен Polymarket
  live_signals.csv      — все сигналы
  live_trades.csv       — виртуальные сделки с P&L
  live_summary.txt      — итоговая статистика

ВАЖНО: Это бумажная торговля. Реальных денег не тратится.
"""

import asyncio
import aiohttp
import json
import time
import csv
import os
from datetime import datetime, timezone
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

# ─────────────────────────────────────────
# НАСТРОЙКИ
# ─────────────────────────────────────────

# Binance
BTC_SYMBOL      = "BTCUSDT"
BTC_WS_URL      = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
BTC_DEPTH_URL   = "https://api.binance.com/api/v3/depth"

# Сигнал: кластер крупных сделок
# Уровни фильтрации (от строгого к мягкому):
#   Строгий:   fi=0.8  count=10  cvd=300k  → ~50 сигналов/день
#   Средний:   fi=0.65 count=5   cvd=100k  → ~200 сигналов/день
#   Мягкий:    fi=0.5  count=3   cvd=50k   → ~500 сигналов/день
# Начинаем с мягкого — собираем данные, потом ужесточаем
FLOW_IMBALANCE_THRESHOLD = 0.5   # перекос потока >= 50%
TRADE_COUNT_MIN          = 3     # минимум сделок за окно
CVD_THRESHOLD            = 50_000   # накопленный CVD за окно (USDT)
SIGNAL_WINDOW_SEC        = 5     # окно для расчёта сигнала

# Варианты выхода из позиции (секунды после сигнала)
# Проверяем все варианты одновременно
EXIT_WINDOWS = [3, 5, 8, 10, 15, 30]

# Виртуальный капитал на сделку
TRADE_SIZE_USD = 200

# Polymarket — 15-минутные BTC рынки
POLYMARKET_API   = "https://clob.polymarket.com"
POLYMARKET_GAMMA = "https://gamma-api.polymarket.com"
POLYMARKET_WS    = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# Как часто опрашиваем через REST (резерв если WS не работает)
POLY_POLL_SEC = 1

# Сколько работает коллектор (секунды). 0 = бесконечно
RUN_DURATION_SEC = 0

# Минимальный интервал между сигналами (секунды)
# Чтобы не спамить сигналами в одном кластере
MIN_SIGNAL_INTERVAL_SEC = 10

# ─────────────────────────────────────────

@dataclass
class Trade:
    ts: float
    price: float
    vol_usdt: float
    side: str  # BUY или SELL
    cvd_delta: float

@dataclass
class Signal:
    ts: float
    btc_price: float
    direction: str         # BUY или SELL
    flow_imb: float
    cvd_5s: float
    trade_count: int
    poly_up_price: Optional[float]
    poly_down_price: Optional[float]
    poly_market_id: Optional[str]
    exits: dict = field(default_factory=dict)  # {seconds: result}

@dataclass
class PolySnapshot:
    ts: float
    market_id: str
    up_price: float
    down_price: float
    btc_strike: float
    minutes_elapsed: float

# ─────────────────────────────────────────

class DataCollector:
    def __init__(self):
        # Буфер последних сделок (скользящее окно)
        self.trades_buffer: deque = deque(maxlen=10000)
        self.cvd_cumulative: float = 0.0

        # Текущее состояние Polymarket
        self.poly_snapshot: Optional[PolySnapshot] = None
        self.current_market_id: Optional[str] = None
        self.current_market_start: Optional[float] = None

        # Активные сигналы ожидающие выхода
        self.pending_signals: list = []
        self.last_signal_ts: float = 0

        # Статистика
        self.total_signals = 0
        self.completed_trades = []

        # CSV файлы
        self._init_csv_files()

        print("DataCollector инициализирован")

    def _init_csv_files(self):
        """Создаёт CSV файлы с заголовками."""

        files = {
            "/app/data/live_binance.csv": [
                "ts", "price", "qty", "vol_usdt", "side", "cvd_delta", "cvd_cumulative"
            ],
            "/app/data/live_polymarket.csv": [
                "ts", "market_id", "up_price", "down_price",
                "btc_strike", "minutes_elapsed", "spread"
            ],
            "/app/data/live_signals.csv": [
                "ts", "btc_price", "direction", "flow_imb_5s",
                "cvd_5s", "trade_count_5s",
                "poly_up", "poly_down", "poly_market_id"
            ],
            "/app/data/live_trades.csv": [
                "signal_ts", "direction", "btc_price_entry",
                "poly_price_entry", "trade_size_usd",
                "exit_3s", "exit_5s", "exit_8s", "exit_10s", "exit_15s", "exit_30s",
                "pnl_3s", "pnl_5s", "pnl_8s", "pnl_10s", "pnl_15s", "pnl_30s",
                "btc_move_3s", "btc_move_5s", "btc_move_8s",
                "btc_move_10s", "btc_move_15s", "btc_move_30s",
                "poly_move_3s", "poly_move_5s", "poly_move_8s",
                "poly_move_10s", "poly_move_15s", "poly_move_30s",
            ],
        }

        for fname, headers in files.items():
            if not os.path.exists(fname):
                with open(fname, "w", newline="", encoding="utf-8") as f:
                    csv.writer(f).writerow(headers)

    def _write_csv(self, fname: str, row: list):
        with open(fname, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(row)

    def on_binance_trade(self, data: dict):
        """Обрабатывает каждую сделку с Binance."""
        try:
            price    = float(data["p"])
            qty      = float(data["q"])
            vol_usdt = price * qty
            # buyer_maker=True → продавец агрессор → SELL тейкер
            side     = "SELL" if data["m"] else "BUY"
            cvd_d    = vol_usdt if side == "BUY" else -vol_usdt
            ts       = data["T"] / 1000  # в секунды

            self.cvd_cumulative += cvd_d

            t = Trade(ts=ts, price=price, vol_usdt=vol_usdt,
                      side=side, cvd_delta=cvd_d)
            self.trades_buffer.append(t)

            self._write_csv("/app/data/live_binance.csv", [
                f"{ts:.3f}", price, qty, f"{vol_usdt:.2f}",
                side, f"{cvd_d:.2f}", f"{self.cvd_cumulative:.2f}"
            ])

        except Exception as e:
            print(f"  [Binance trade error] {e}")

    def compute_signal(self) -> Optional[Signal]:
        """
        Считает метрики за последние SIGNAL_WINDOW_SEC секунд.
        Возвращает Signal если условия выполнены, иначе None.
        """
        now = time.time()
        cutoff = now - SIGNAL_WINDOW_SEC

        # Сделки за окно
        window = [t for t in self.trades_buffer if t.ts >= cutoff]

        if len(window) < TRADE_COUNT_MIN:
            return None

        buy_vol  = sum(t.vol_usdt for t in window if t.side == "BUY")
        sell_vol = sum(t.vol_usdt for t in window if t.side == "SELL")
        total    = buy_vol + sell_vol
        cvd_5s   = sum(t.cvd_delta for t in window)

        if total == 0:
            return None

        flow_imb = (buy_vol - sell_vol) / total
        direction = "BUY" if flow_imb > 0 else "SELL"

        # Проверяем пороги
        if abs(flow_imb) < FLOW_IMBALANCE_THRESHOLD:
            return None
        if abs(cvd_5s) < CVD_THRESHOLD:
            return None
        if len(window) < TRADE_COUNT_MIN:
            return None

        # Минимальный интервал между сигналами
        if now - self.last_signal_ts < MIN_SIGNAL_INTERVAL_SEC:
            return None

        btc_price = window[-1].price

        # Текущая цена Polymarket
        poly_up = poly_down = poly_id = None
        if self.poly_snapshot:
            poly_up   = self.poly_snapshot.up_price
            poly_down = self.poly_snapshot.down_price
            poly_id   = self.poly_snapshot.market_id

        return Signal(
            ts=now,
            btc_price=btc_price,
            direction=direction,
            flow_imb=flow_imb,
            cvd_5s=cvd_5s,
            trade_count=len(window),
            poly_up_price=poly_up,
            poly_down_price=poly_down,
            poly_market_id=poly_id,
        )

    def record_signal(self, sig: Signal):
        """Записывает сигнал и добавляет в очередь на выход."""
        self.last_signal_ts = sig.ts
        self.total_signals += 1
        self.pending_signals.append(sig)

        self._write_csv("/app/data/live_signals.csv", [
            f"{sig.ts:.3f}", f"{sig.btc_price:.2f}", sig.direction,
            f"{sig.flow_imb:.4f}", f"{sig.cvd_5s:.0f}", sig.trade_count,
            sig.poly_up_price, sig.poly_down_price, sig.poly_market_id
        ])

        poly_str = ""
        if sig.poly_up_price:
            entry_price = sig.poly_up_price if sig.direction == "BUY" else sig.poly_down_price
            poly_str = f" | Poly entry: {entry_price:.2f}¢"

        print(f"\n{'='*60}")
        print(f"  🔔 СИГНАЛ #{self.total_signals} | {sig.direction}")
        print(f"  BTC: ${sig.btc_price:,.2f}")
        print(f"  Flow imbalance: {sig.flow_imb:+.3f} | CVD: ${sig.cvd_5s:,.0f}")
        print(f"  Сделок за {SIGNAL_WINDOW_SEC}с: {sig.trade_count}")
        if sig.poly_up_price:
            print(f"  Polymarket UP: {sig.poly_up_price:.2f}¢ | DOWN: {sig.poly_down_price:.2f}¢")
        print(f"  Ждём выходы через: {EXIT_WINDOWS}с")
        print(f"{'='*60}")

    def check_exits(self, btc_price_now: float):
        """
        Проверяет все pending сигналы — не пора ли фиксировать результат.
        """
        now = time.time()
        still_pending = []

        for sig in self.pending_signals:
            elapsed = now - sig.ts
            entry_price = (sig.poly_up_price if sig.direction == "BUY"
                          else sig.poly_down_price)

            for ex_sec in EXIT_WINDOWS:
                if ex_sec in sig.exits:
                    continue
                if elapsed >= ex_sec:

                    # BTC движение (всегда в направлении сигнала)
                    btc_move_pct = (btc_price_now - sig.btc_price) / sig.btc_price * 100
                    if sig.direction == "SELL":
                        btc_move_pct = -btc_move_pct

                    # Polymarket движение
                    poly_move  = None
                    poly_exit  = None
                    valid_snap = False

                    if self.poly_snapshot and entry_price:
                        snap = self.poly_snapshot

                        # Проверяем что снимок свежий (не старше 15 секунд)
                        snap_age = now - snap.ts
                        if snap_age > 15:
                            print(f"  [{ex_sec}s] ⚠ Снимок Poly устарел ({snap_age:.0f}с) — пропускаем")
                            sig.exits[ex_sec] = {
                                "btc_move": btc_move_pct,
                                "poly_move": None,
                                "poly_exit": None,
                                "pnl": None,
                                "elapsed": elapsed,
                                "invalid": "stale_snapshot",
                            }
                            continue

                        # Защита от смены рынка: цена не должна прыгать > 30¢ за раз
                        if sig.direction == "BUY":
                            poly_exit = snap.up_price
                            raw_move  = poly_exit - entry_price
                        else:
                            poly_exit = snap.down_price
                            raw_move  = poly_exit - entry_price

                        # Валидация: движение > 30¢ = скорее всего смена рынка
                        if abs(raw_move) > 30:
                            poly_move = None
                            poly_exit = None
                            # Помечаем как невалидную — смена рынка
                            sig.exits[ex_sec] = {
                                "btc_move": btc_move_pct,
                                "poly_move": None,
                                "poly_exit": None,
                                "pnl": None,
                                "elapsed": elapsed,
                                "invalid": "market_change",
                            }
                            print(f"  [{ex_sec}s] ⚠ Смена рынка (движение {raw_move:+.1f}¢) — сделка исключена")
                            continue
                        else:
                            poly_move  = raw_move
                            valid_snap = True

                    # Баг 1: правильный P&L
                    # BUY UP: прибыль если UP вырос (poly_move > 0)
                    # SELL DOWN: прибыль если DOWN вырос (poly_move > 0, т.к. мы держим DOWN)
                    # В обоих случаях: profit = contracts * poly_move
                    # Нет инверсии — мы ПОКУПАЕМ нужный токен в направлении сигнала
                    pnl = None
                    if poly_move is not None and entry_price and valid_snap:
                        contracts  = TRADE_SIZE_USD / (entry_price / 100)
                        gross      = contracts * (poly_move / 100)
                        commission = TRADE_SIZE_USD * 0.018
                        pnl        = gross - commission

                    sig.exits[ex_sec] = {
                        "btc_move": btc_move_pct,
                        "poly_move": poly_move,
                        "poly_exit": poly_exit,
                        "pnl": pnl,
                        "elapsed": elapsed,
                    }

                    if pnl is not None:
                        status = "✓" if pnl > 0 else "✗"
                        print(f"  [{ex_sec}s] BTC {btc_move_pct:+.3f}% | "
                              f"Poly {poly_move:+.2f}¢ | P&L ${pnl:+.2f} {status}")
                    elif poly_move is None and ex_sec == EXIT_WINDOWS[-1]:
                        print(f"  [{ex_sec}s] BTC {btc_move_pct:+.3f}% | Poly: НЕТ ДАННЫХ")

            # Все выходы записаны?
            if all(ex in sig.exits for ex in EXIT_WINDOWS):
                self._finalize_trade(sig)
            else:
                still_pending.append(sig)

        self.pending_signals = still_pending

    def _finalize_trade(self, sig: Signal):
        """Финализирует сделку — записывает в CSV."""
        entry_price = (sig.poly_up_price if sig.direction == "BUY"
                      else sig.poly_down_price)

        row = [
            f"{sig.ts:.3f}",
            sig.direction,
            f"{sig.btc_price:.2f}",
            f"{entry_price:.4f}" if entry_price else "",
            TRADE_SIZE_USD,
        ]

        for ex in EXIT_WINDOWS:
            data = sig.exits.get(ex, {})
            row.append(f"{data.get('poly_exit', '')}")

        for ex in EXIT_WINDOWS:
            data = sig.exits.get(ex, {})
            pnl  = data.get("pnl")
            row.append(f"{pnl:.4f}" if pnl is not None else "")

        for ex in EXIT_WINDOWS:
            data = sig.exits.get(ex, {})
            row.append(f"{data.get('btc_move', ''):.4f}" if data.get('btc_move') is not None else "")

        for ex in EXIT_WINDOWS:
            data = sig.exits.get(ex, {})
            row.append(f"{data.get('poly_move', ''):.4f}" if data.get('poly_move') is not None else "")

        self._write_csv("/app/data/live_trades.csv", row)
        self.completed_trades.append(sig)

        # Итоговый P&L по сделке
        pnls = {ex: sig.exits[ex].get("pnl") for ex in EXIT_WINDOWS
                if sig.exits[ex].get("pnl") is not None}
        if pnls:
            best_ex  = max(pnls, key=lambda x: pnls[x])
            best_pnl = pnls[best_ex]
            print(f"\n  📊 Сделка завершена | Лучший выход: {best_ex}s → ${best_pnl:+.2f}")


# ─────────────────────────────────────────
# POLYMARKET
# ─────────────────────────────────────────

def get_current_15m_slug(crypto: str = "btc") -> list:
    """
    Генерирует слаги для текущего и следующего 15-минутного интервала.
    Формат: btc-updown-15m-{unix_timestamp}
    Timestamp = начало текущего 15-минутного окна (округление вниз).
    """
    now = int(time.time())
    interval = 15 * 60  # 900 секунд

    # Текущий интервал и соседние (на случай перехода между интервалами)
    current = (now // interval) * interval
    slugs = []
    for offset in [-1, 0, 1]:
        ts = current + offset * interval
        slugs.append(f"{crypto}-updown-15m-{ts}")
    return slugs


async def find_btc_market(session: aiohttp.ClientSession) -> Optional[dict]:
    """
    Находит активный 15-минутный BTC рынок на Polymarket.
    Использует точный slug формат: btc-updown-15m-{timestamp}
    """
    slugs = get_current_15m_slug("btc")

    for slug in slugs:
        try:
            url = f"{POLYMARKET_GAMMA}/markets?slug={slug}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    continue
                data = await r.json()

            # Gamma возвращает список или объект
            markets = data if isinstance(data, list) else data.get("markets", [data])

            for m in markets:
                if not m.get("active", False):
                    continue
                if m.get("closed", True):
                    continue
                # Проверяем что рынок текущий (не старше 15 минут)
                market_slug = m.get("slug", "")
                parts = market_slug.split("-")
                market_is_current = False
                for part in reversed(parts):
                    if part.isdigit() and len(part) >= 9:
                        market_start = int(part)
                        age_minutes = (time.time() - market_start) / 60
                        if 0 <= age_minutes <= 15:
                            market_is_current = True
                        break
                if not market_is_current:
                    print(f"  [Poly] Пропускаю старый рынок: {market_slug}")
                    continue
                outcomes = m.get("outcomes", [])
                prices   = m.get("outcomePrices", [])
                if len(outcomes) >= 2 and len(prices) >= 2:
                    print(f"[Polymarket] Найден текущий рынок: {m.get('question', slug)}")
                    return m

        except Exception as e:
            print(f"  [Poly find slug {slug}] {e}")

    # Резерв: поиск по keyword через /markets
    try:
        url = f"{POLYMARKET_GAMMA}/markets"
        params = {
            "active": "true",
            "closed": "false",
            "limit": 50,
            "tag_slug": "crypto",
        }
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return None
            markets = await r.json()
            if not isinstance(markets, list):
                markets = markets.get("markets", [])

        for m in markets:
            q = (m.get("question") or "").lower()
            if "btc" in q and ("up" in q or "higher" in q) and "15" in q:
                outcomes = m.get("outcomes", [])
                prices   = m.get("outcomePrices", [])
                if len(outcomes) >= 2 and len(prices) >= 2:
                    print(f"[Polymarket] Найден через поиск: {m.get('question', '')}")
                    return m

    except Exception as e:
        print(f"  [Poly keyword search error] {e}")

    return None


async def get_poly_prices(session: aiohttp.ClientSession,
                          market: dict,
                          collector: DataCollector):
    """
    Получает текущие цены UP/DOWN из поля outcomePrices (Gamma API).
    Также пробует CLOB API для более точных цен.
    """
    try:
        now = time.time()

        # Способ 1: цены прямо из market объекта (Gamma API)
        import json as _json

        outcomes_raw = market.get("outcomes", [])
        prices_raw   = market.get("outcomePrices", [])

        # Gamma API может вернуть строку вместо списка — парсим
        if isinstance(outcomes_raw, str):
            try: outcomes_raw = _json.loads(outcomes_raw)
            except: outcomes_raw = []
        if isinstance(prices_raw, str):
            try: prices_raw = _json.loads(prices_raw)
            except: prices_raw = []

        outcomes = outcomes_raw
        prices   = prices_raw
        up_price = down_price = None

        for i, outcome in enumerate(outcomes):
            o = str(outcome).lower()
            try:
                p = float(prices[i]) * 100 if i < len(prices) else None
            except (ValueError, TypeError):
                p = None
            if o in ("up", "higher", "yes") and p is not None:
                up_price = p
            elif o in ("down", "lower", "no") and p is not None:
                down_price = p

        # Способ 2: свежие цены через CLOB midpoint (без авторизации)
        clob_ids = market.get("clobTokenIds", [])
        if len(clob_ids) >= 2:
            try:
                for i, token_id in enumerate(clob_ids[:2]):
                    url = f"{POLYMARKET_API}/midpoint?token_id={token_id}"
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=3)) as r:
                        if r.status == 200:
                            d = await r.json()
                            mid = float(d.get("mid", 0)) * 100
                            outcome = (outcomes[i] if i < len(outcomes) else "").lower()
                            if outcome in ("up", "higher", "yes"):
                                up_price = mid
                            elif outcome in ("down", "lower", "no"):
                                down_price = mid
            except:
                pass  # Fallback на Gamma цены

        if up_price is None or down_price is None:
            # Если одна цена есть — вычисляем вторую
            if up_price is not None:
                down_price = 100 - up_price
            elif down_price is not None:
                up_price = 100 - down_price
            else:
                return

        # Время с начала рынка — вычисляем из slug (самый надёжный способ)
        # Slug формат: btc-updown-15m-{unix_timestamp}
        mins_elapsed = 0.0
        slug = market.get("slug", "")
        try:
            parts = slug.split("-")
            # Ищем последнюю числовую часть
            for part in reversed(parts):
                if part.isdigit() and len(part) >= 9:
                    market_start_ts = int(part)
                    mins_elapsed = (time.time() - market_start_ts) / 60
                    # Санity check: 15-минутный рынок, прошло не больше 15 минут
                    if mins_elapsed > 15:
                        # Старый рынок — пересчитываем на текущий интервал
                        current_start = (int(time.time()) // 900) * 900
                        mins_elapsed = (time.time() - current_start) / 60
                    break
        except:
            pass

        # Fallback: считаем из startDate если slug не дал результат
        if mins_elapsed == 0.0:
            start_ts = market.get("startDate") or market.get("start_date_iso") or market.get("startDateIso")
            if start_ts:
                try:
                    from datetime import datetime
                    st = datetime.fromisoformat(str(start_ts).replace("Z", "+00:00"))
                    mins_elapsed = (datetime.now(timezone.utc) - st).total_seconds() / 60
                    if mins_elapsed > 15:
                        current_start = (int(time.time()) // 900) * 900
                        mins_elapsed = (time.time() - current_start) / 60
                except:
                    pass

        condition_id = market.get("conditionId") or market.get("condition_id") or market.get("slug", "")

        snap = PolySnapshot(
            ts=now,
            market_id=condition_id,
            up_price=round(up_price, 4),
            down_price=round(down_price, 4),
            btc_strike=float(market.get("strike") or 0),
            minutes_elapsed=round(mins_elapsed, 2),
        )
        collector.poly_snapshot = snap

        spread = abs(up_price + down_price - 100)
        collector._write_csv("/app/data/live_polymarket.csv", [
            f"{now:.3f}", condition_id,
            f"{up_price:.4f}", f"{down_price:.4f}",
            snap.btc_strike, f"{mins_elapsed:.2f}",
            f"{spread:.4f}",
        ])

    except Exception as e:
        print(f"  [Poly prices error] {e}")


# ─────────────────────────────────────────
# ОСНОВНЫЕ ЗАДАЧИ (asyncio)
# ─────────────────────────────────────────

async def binance_ws_task(collector: DataCollector):
    """WebSocket поток сделок Binance."""
    import websockets

    print(f"[Binance WS] Подключаюсь к {BTC_WS_URL}...")
    retry_delay = 1

    while True:
        try:
            async with websockets.connect(BTC_WS_URL, ping_interval=20) as ws:
                print("[Binance WS] Подключён ✓")
                retry_delay = 1
                async for msg in ws:
                    data = json.loads(msg)
                    collector.on_binance_trade(data)
        except Exception as e:
            print(f"[Binance WS] Ошибка: {e} — реконнект через {retry_delay}с")
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 30)


async def signal_task(collector: DataCollector):
    """Каждую секунду проверяет сигналы и выходы."""
    last_btc_price = 0

    while True:
        await asyncio.sleep(1)

        # Текущая цена BTC из буфера
        if collector.trades_buffer:
            last_btc_price = collector.trades_buffer[-1].price

        # Проверяем выходы по pending сигналам
        if collector.pending_signals and last_btc_price:
            collector.check_exits(last_btc_price)

        # Проверяем новый сигнал
        sig = collector.compute_signal()
        if sig:
            collector.record_signal(sig)


async def polymarket_fast_poll(collector: DataCollector, market: dict):
    """
    Получает цены через Gamma API outcomePrices.
    Обновляем рынок через API каждые 3 секунды — Gamma кешируется,
    но при каждом запросе отдаёт свежий объект рынка.
    Это единственный метод который работает через VPN.
    """
    cid  = market.get('conditionId', '')
    slug = market.get('slug', '')

    print(f'[Poly POLL] Старт | рынок: {market.get("question","")}')

    connector = aiohttp.TCPConnector(ssl=False, limit=5)
    async with aiohttp.ClientSession(connector=connector) as session:
        error_count = 0
        first_price = True

        while True:
            try:
                # Запрашиваем свежий рынок напрямую по slug
                url = f"{POLYMARKET_GAMMA}/markets?slug={slug}"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                    if r.status != 200:
                        await asyncio.sleep(3)
                        continue
                    data = await r.json()

                markets_list = data if isinstance(data, list) else data.get('markets', [data])
                if not markets_list:
                    await asyncio.sleep(3)
                    continue

                m = markets_list[0]

                import json as _j
                outcomes_raw = m.get('outcomes', [])
                prices_raw   = m.get('outcomePrices', [])
                if isinstance(outcomes_raw, str):
                    try: outcomes_raw = _j.loads(outcomes_raw)
                    except: outcomes_raw = []
                if isinstance(prices_raw, str):
                    try: prices_raw = _j.loads(prices_raw)
                    except: prices_raw = []

                up_price = down_price = None
                for i, outcome in enumerate(outcomes_raw):
                    o = str(outcome).lower()
                    try:
                        p = float(prices_raw[i]) * 100 if i < len(prices_raw) else None
                    except:
                        p = None
                    if p is None:
                        continue
                    if o in ('up', 'higher', 'yes'):
                        up_price = p
                    elif o in ('down', 'lower', 'no'):
                        down_price = p

                if up_price is None and down_price is None:
                    error_count += 1
                    if error_count % 10 == 0:
                        print(f'[Poly POLL] Цены не найдены. outcomes={outcomes_raw} prices={prices_raw}')
                    await asyncio.sleep(3)
                    continue

                if up_price is None:
                    up_price = 100 - down_price
                if down_price is None:
                    down_price = 100 - up_price

                now  = time.time()
                mins = 0.0
                for part in reversed(slug.split('-')):
                    if part.isdigit() and len(part) >= 9:
                        mins = (now - int(part)) / 60
                        break

                old_snap = collector.poly_snapshot
                new_up   = round(up_price, 2)
                new_down = round(down_price, 2)

                snap = PolySnapshot(
                    ts=now,
                    market_id=cid,
                    up_price=new_up,
                    down_price=new_down,
                    btc_strike=0,
                    minutes_elapsed=round(mins, 2),
                )
                collector.poly_snapshot = snap

                # Логируем первую цену и каждое изменение
                if first_price:
                    print(f'[Poly POLL] ✓ Первая цена: UP={new_up:.1f}¢ DOWN={new_down:.1f}¢')
                    first_price = False
                    error_count = 0
                elif old_snap and abs(new_up - old_snap.up_price) >= 0.5:
                    diff = new_up - old_snap.up_price
                    print(f'  [Poly] UP: {old_snap.up_price:.1f}¢ → {new_up:.1f}¢ ({diff:+.1f}¢)')

                # Пишем в CSV при каждом изменении
                if not old_snap or new_up != old_snap.up_price:
                    collector._write_csv('/app/data/live_polymarket.csv', [
                        f'{now:.3f}', cid,
                        f'{new_up:.4f}', f'{new_down:.4f}',
                        0, f'{mins:.2f}', '0',
                    ])

            except Exception as e:
                error_count += 1
                if error_count <= 3 or error_count % 20 == 0:
                    print(f'[Poly POLL error #{error_count}] {e}')

            await asyncio.sleep(3)  # Gamma кешируется — чаще нет смысла


async def polymarket_task(collector: DataCollector):
    """
    Управляет подключением к Polymarket.
    1. Находит текущий рынок через Gamma API
    2. Подключается к CLOB WebSocket для реальных цен
    3. При смене рынка — переподключается
    """
    print('[Polymarket] Запуск...')

    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        current_market = None
        last_market_id = None
        poll_task = None

        while True:
            now = time.time()

            # Ищем текущий рынок каждые 30 секунд
            new_market = await find_btc_market(session)

            if new_market:
                new_id = new_market.get('conditionId', '')

                # Рынок сменился — переподключаем WS
                if new_id != last_market_id:
                    title = new_market.get('question', '')
                    print(f'[Polymarket] Новый рынок: {title}')
                    collector.current_market_id = new_id

                    if poll_task and not poll_task.done():
                        poll_task.cancel()
                        await asyncio.sleep(0.1)

                    # Запускаем быстрый REST поллинг
                    poll_task = asyncio.create_task(
                        polymarket_fast_poll(collector, new_market)
                    )
                    current_market = new_market
                    last_market_id = new_id

                else:
                    # Тот же рынок — поллинг уже идёт
                    if poll_task and poll_task.done():
                        print('[Polymarket] Поллинг упал — перезапуск...')
                        poll_task = asyncio.create_task(
                            polymarket_fast_poll(collector, current_market)
                        )

            else:
                print('[Polymarket] Рынок не найден — жду...')

            await asyncio.sleep(10)


async def stats_task(collector: DataCollector):
    """Каждые 30 секунд выводит статистику."""
    start = time.time()

    while True:
        await asyncio.sleep(30)

        elapsed_min = (time.time() - start) / 60
        n_trades    = len(collector.completed_trades)

        print(f"\n{'─'*50}")
        print(f"  ⏱  Работаем: {elapsed_min:.1f} мин")
        print(f"  📡 Сделок в буфере: {len(collector.trades_buffer)}")
        n_invalid = sum(
            1 for t in collector.completed_trades
            for ex in EXIT_WINDOWS
            if ex in t.exits and t.exits[ex].get("invalid") == "market_change"
        ) // max(1, len(EXIT_WINDOWS))
        # Считаем сделки с ценой Poly и без
        with_poly  = sum(1 for t in collector.completed_trades if t.poly_up_price is not None)
        no_poly    = n_trades - with_poly
        poly_pct   = with_poly/n_trades*100 if n_trades > 0 else 0
        print(f"  🔔 Сигналов: {collector.total_signals} | завершено: {n_trades} | ожидает: {len(collector.pending_signals)}")
        print(f"  💰 Сделок с ценой Poly: {with_poly} ({poly_pct:.0f}%) | без цены: {no_poly}")
        if no_poly > 0 and n_trades > 0 and poly_pct < 50:
            print(f"  ⚠ ВНИМАНИЕ: более половины сделок без цены Poly!")

        if collector.poly_snapshot:
            s = collector.poly_snapshot
            age = time.time() - s.ts
            print(f"  📊 Polymarket: UP={s.up_price:.1f}¢ DOWN={s.down_price:.1f}¢ "
                  f"({s.minutes_elapsed:.1f} мин рынка | обновлено {age:.1f}с назад)")
        else:
            print(f"  📊 Polymarket: НЕТ ДАННЫХ (poly_snapshot = None)")

        # P&L статистика
        if n_trades > 0:
            print(f"\n  === P&L СТАТИСТИКА (бумажная торговля) ===")
            for ex in EXIT_WINDOWS:
                pnls = [
                    t.exits[ex]["pnl"]
                    for t in collector.completed_trades
                    if ex in t.exits
                    and t.exits[ex].get("pnl") is not None
                    and t.exits[ex].get("invalid") is None
                ]
                if not pnls:
                    continue
                wins   = sum(1 for p in pnls if p > 0)
                total  = sum(pnls)
                avg    = total / len(pnls)
                print(f"  {ex:>3}s: {wins}/{len(pnls)} ({wins/len(pnls)*100:.0f}%) | "
                      f"avg=${avg:+.2f} | total=${total:+.2f}")

        print(f"{'─'*50}\n")


async def main():
    print("="*55)
    print("  LIVE COLLECTOR: Binance + Polymarket")
    print("  Бумажная торговля — реальных денег нет")
    print("="*55)

    # Диагностика при старте
    print("\n[ДИАГНОСТИКА] Проверяю подключения...")
    import aiohttp as _aio
    async def _check():
        try:
            async with _aio.ClientSession() as s:
                async with s.get("https://api.binance.com/api/v3/time", timeout=_aio.ClientTimeout(total=5)) as r:
                    print(f"  Binance API: {'OK' if r.status==200 else 'ОШИБКА ' + str(r.status)}")
        except Exception as e:
            print(f"  Binance API: ОШИБКА — {e}")
        try:
            async with _aio.ClientSession() as s:
                async with s.get("https://gamma-api.polymarket.com/markets?limit=1", timeout=_aio.ClientTimeout(total=5)) as r:
                    print(f"  Gamma API: {'OK' if r.status==200 else 'ОШИБКА ' + str(r.status)}")
        except Exception as e:
            print(f"  Gamma API: ОШИБКА — {e}")
        try:
            async with _aio.ClientSession() as s:
                async with s.get("https://clob.polymarket.com/markets?limit=1", timeout=_aio.ClientTimeout(total=5)) as r:
                    print(f"  CLOB API: {'OK' if r.status==200 else 'ОШИБКА ' + str(r.status)}")
        except Exception as e:
            print(f"  CLOB API: ОШИБКА — {e}")
    await _check()
    print("[ДИАГНОСТИКА] Готово\n")
    print(f"  Сигнал: flow_imb>={FLOW_IMBALANCE_THRESHOLD} | "
          f"count>={TRADE_COUNT_MIN} | cvd>=${CVD_THRESHOLD:,}")
    print(f"  Окно сигнала: {SIGNAL_WINDOW_SEC}с")
    print(f"  Выходы: {EXIT_WINDOWS}с")
    print(f"  Капитал на сделку: ${TRADE_SIZE_USD}")
    print(f"  Мин. интервал между сигналами: {MIN_SIGNAL_INTERVAL_SEC}с")
    print("="*55)
    print("  Ctrl+C для остановки\n")

    collector = DataCollector()

    tasks = [
        asyncio.create_task(binance_ws_task(collector)),
        asyncio.create_task(signal_task(collector)),
        asyncio.create_task(polymarket_task(collector)),  # управляет и REST и WS
        asyncio.create_task(stats_task(collector)),
    ]

    try:
        if RUN_DURATION_SEC > 0:
            await asyncio.wait_for(
                asyncio.gather(*tasks),
                timeout=RUN_DURATION_SEC
            )
        else:
            await asyncio.gather(*tasks)

    except (asyncio.TimeoutError, KeyboardInterrupt):
        print("\n\nОстановка...")
        for t in tasks:
            t.cancel()

    finally:
        # Финальная статистика
        print("\n" + "="*55)
        print("  ФИНАЛЬНАЯ СТАТИСТИКА")
        print("="*55)

        all_pnls = {}
        for ex in EXIT_WINDOWS:
            pnls = [
                t.exits[ex]["pnl"]
                for t in collector.completed_trades
                if ex in t.exits
                and t.exits[ex].get("pnl") is not None
                and t.exits[ex].get("invalid") is None
            ]
            all_pnls[ex] = pnls

        summary_lines = [
            f"Всего сигналов: {collector.total_signals}",
            f"Завершённых сделок: {len(collector.completed_trades)}",
            "",
            "P&L по таймфреймам выхода:",
        ]

        for ex in EXIT_WINDOWS:
            pnls = all_pnls.get(ex, [])
            if not pnls:
                continue
            wins  = sum(1 for p in pnls if p > 0)
            total = sum(pnls)
            avg   = total / len(pnls)
            line  = (f"  {ex}s: {wins}/{len(pnls)} ({wins/len(pnls)*100:.0f}%) | "
                     f"avg=${avg:+.2f} | total=${total:+.2f}")
            print(line)
            summary_lines.append(line)

        with open("/app/data/live_summary.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(summary_lines))

        print("\nФайлы сохранены:")
        print("  live_binance.csv | live_polymarket.csv")
        print("  live_signals.csv | live_trades.csv | live_summary.txt")


if __name__ == "__main__":
    asyncio.run(main())
