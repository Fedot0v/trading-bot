"""
Binance Deep Analyzer v2
=========================
Анализирует за 48 часов:

СДЕЛКИ:
  - Агрегированные сделки (aggTrades)
  - Суммарный поток BUY/SELL за скользящие окна 1s/3s/5s/10s
  - Дисбаланс потока (flow imbalance)
  - Кластеры крупных сделок (несколько крупных за 1-2 секунды)

СТАКАН (Order Book):
  - Снимок стакана каждые 5 секунд (top 10 уровней)
  - Дисбаланс стакана (bid volume vs ask volume)
  - Изменение стакана между снимками
  - Появление/исчезновение крупных стен

ВЗАИМОСВЯЗИ:
  - Поток сделок + дисбаланс стакана → движение цены
  - Стакан меняется ДО крупной сделки?
  - Какой из сигналов работает лучше как предиктор

ВЫХОД:
  - deep_trades.csv         — все сделки с rolling метриками
  - deep_orderbook.csv      — снимки стакана
  - deep_signals.csv        — все сигналы с результатами
  - deep_summary.txt        — итоговая статистика

Запуск:
  pip install requests pandas numpy
  python binance_deep_analyzer.py
"""

import requests
import pandas as pd
import numpy as np
import time
import json
from datetime import datetime, timezone, timedelta

# ─────────────────────────────────────────
# НАСТРОЙКИ
# ─────────────────────────────────────────

SYMBOL = "BTCUSDT"
HOURS = 24

# Пороги для сигналов
LARGE_TRADE_USDT = 200_000       # крупная одиночная сделка
CLUSTER_WINDOW_SEC = 3           # окно для кластера сделок
CLUSTER_TOTAL_USDT = 500_000     # суммарный объём кластера

# Flow imbalance порог (0 до 1)
FLOW_IMBALANCE_THRESHOLD = 0.6

# Дисбаланс стакана порог
OB_IMBALANCE_THRESHOLD = 0.6

# Скользящие окна для анализа потока (секунды)
ROLLING_WINDOWS = [1, 3, 5, 10]

# Смотрим что происходит с ценой через N секунд после сигнала
LOOK_AHEAD = [2, 3, 5, 10, 15, 30]

# Минимальное движение чтобы считать импульсом
MIN_IMPULSE_PCT = 0.05

# Как часто снимаем стакан (секунды)
OB_SNAPSHOT_INTERVAL = 5

BASE_URL = "https://api.binance.com"

# ─────────────────────────────────────────


def get_trades(symbol: str, hours: int) -> pd.DataFrame:
    """Выгружает все агрегированные сделки за N часов."""

    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = end_ms - hours * 3600 * 1000

    all_trades = []
    cur = start_ms
    batch = 0

    print(f"[1/3] Выгружаю сделки за {hours} часов...")

    while cur < end_ms:
        chunk_end = min(cur + 3_600_000, end_ms)
        url = f"{BASE_URL}/api/v3/aggTrades"
        params = {"symbol": symbol, "startTime": cur, "endTime": chunk_end, "limit": 1000}

        try:
            r = requests.get(url, params=params, timeout=15)
            r.raise_for_status()
            trades = r.json()
        except Exception as e:
            print(f"\n  Ошибка: {e}, повтор...")
            time.sleep(2)
            continue

        if not trades:
            cur = chunk_end + 1
            continue

        all_trades.extend(trades)
        cur = trades[-1]["T"] + 1
        batch += 1
        print(f"  Загружено: {len(all_trades):,} сделок (батч {batch})", end="\r")
        time.sleep(0.15)

    print(f"\n  Итого сделок: {len(all_trades):,}")

    df = pd.DataFrame(all_trades)
    df = df.rename(columns={"T": "ts_ms", "p": "price", "q": "qty", "m": "buyer_maker", "a": "id"})
    df["ts"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
    df["price"] = df["price"].astype(float)
    df["qty"] = df["qty"].astype(float)
    df["vol_usdt"] = df["price"] * df["qty"]
    # Тейкер-сторона: buyer_maker=True → продавец агрессор (SELL тейкер)
    df["side"] = df["buyer_maker"].apply(lambda x: "SELL" if x else "BUY")
    df = df.sort_values("ts").reset_index(drop=True)
    return df


def compute_rolling_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Для каждой сделки считает метрики за скользящие окна ДО неё:
    - buy_vol_Ns, sell_vol_Ns — объём покупок/продаж за N секунд до
    - flow_imbalance_Ns — (buy-sell)/(buy+sell)
    - trade_count_Ns — количество сделок за N секунд
    - large_count_Ns — количество крупных сделок за N секунд
    """

    print("[2/3] Считаю rolling метрики...")

    df = df.copy()
    df_indexed = df.set_index("ts")

    for w in ROLLING_WINDOWS:
        buy_vol = []
        sell_vol = []
        count = []
        large_count = []

        for i, row in df.iterrows():
            t = row["ts"]
            window = df_indexed[
                (df_indexed.index >= t - pd.Timedelta(seconds=w)) &
                (df_indexed.index < t)
            ]
            bv = window[window["side"] == "BUY"]["vol_usdt"].sum()
            sv = window[window["side"] == "SELL"]["vol_usdt"].sum()
            buy_vol.append(bv)
            sell_vol.append(sv)
            count.append(len(window))
            large_count.append((window["vol_usdt"] >= LARGE_TRADE_USDT).sum())

            if i % 5000 == 0:
                print(f"  Прогресс: {i}/{len(df)}", end="\r")

        df[f"buy_vol_{w}s"] = buy_vol
        df[f"sell_vol_{w}s"] = sell_vol
        df[f"flow_imb_{w}s"] = (
            (np.array(buy_vol) - np.array(sell_vol)) /
            (np.array(buy_vol) + np.array(sell_vol) + 1)
        )
        df[f"trade_count_{w}s"] = count
        df[f"large_count_{w}s"] = large_count

    print(f"\n  Rolling метрики готовы")
    return df


def compute_price_outcomes(df: pd.DataFrame) -> pd.DataFrame:
    """Для каждой сделки считает движение цены через N секунд после."""

    df = df.copy()
    df_indexed = df.set_index("ts")

    for la in LOOK_AHEAD:
        prices_after = []
        for i, row in df.iterrows():
            t = row["ts"]
            future = df_indexed[
                (df_indexed.index > t) &
                (df_indexed.index <= t + pd.Timedelta(seconds=la))
            ]
            if future.empty:
                prices_after.append(None)
            else:
                prices_after.append(future.iloc[-1]["price"])

        df[f"price_{la}s"] = prices_after
        df[f"move_{la}s"] = (df[f"price_{la}s"] - df["price"]) / df["price"] * 100

    return df


def get_orderbook_snapshots(symbol: str, hours: int) -> pd.DataFrame:
    """
    Получает текущий снимок стакана (реальное время).
    Для истории используем klines как прокси глубины.
    Также делаем N снимков текущего стакана для демонстрации структуры.
    """

    print("[3/3] Получаю данные стакана...")
    snapshots = []

    # Текущий стакан — 20 снимков с интервалом 5 секунд
    print("  Делаю снимки текущего стакана (20 снимков × 5 сек)...")
    for i in range(20):
        try:
            r = requests.get(
                f"{BASE_URL}/api/v3/depth",
                params={"symbol": symbol, "limit": 20},
                timeout=10
            )
            r.raise_for_status()
            ob = r.json()

            bids = [(float(p), float(q)) for p, q in ob["bids"]]
            asks = [(float(p), float(q)) for p, q in ob["asks"]]

            bid_vol = sum(p * q for p, q in bids)
            ask_vol = sum(p * q for p, q in asks)
            bid_vol_top5 = sum(p * q for p, q in bids[:5])
            ask_vol_top5 = sum(p * q for p, q in asks[:5])

            best_bid = bids[0][0] if bids else None
            best_ask = asks[0][0] if asks else None
            spread = (best_ask - best_bid) if (best_bid and best_ask) else None
            spread_pct = spread / best_bid * 100 if (spread and best_bid) else None

            # Крупные стены (> $500k на одном уровне)
            big_bids = [(p, q) for p, q in bids if p * q > 500_000]
            big_asks = [(p, q) for p, q in asks if p * q > 500_000]

            snapshots.append({
                "ts": datetime.now(timezone.utc),
                "best_bid": best_bid,
                "best_ask": best_ask,
                "spread_pct": spread_pct,
                "bid_vol_top10": bid_vol,
                "ask_vol_top10": ask_vol,
                "bid_vol_top5": bid_vol_top5,
                "ask_vol_top5": ask_vol_top5,
                "ob_imbalance": (bid_vol - ask_vol) / (bid_vol + ask_vol + 1),
                "ob_imbalance_top5": (bid_vol_top5 - ask_vol_top5) / (bid_vol_top5 + ask_vol_top5 + 1),
                "big_bid_count": len(big_bids),
                "big_ask_count": len(big_asks),
                "big_bid_vol": sum(p * q for p, q in big_bids),
                "big_ask_vol": sum(p * q for p, q in big_asks),
            })

            print(f"  Снимок {i+1}/20 | bid_imb={snapshots[-1]['ob_imbalance']:.3f} | spread={spread_pct:.4f}%", end="\r")
            time.sleep(OB_SNAPSHOT_INTERVAL)

        except Exception as e:
            print(f"\n  Ошибка снимка: {e}")
            time.sleep(2)

    print(f"\n  Снимков стакана: {len(snapshots)}")
    return pd.DataFrame(snapshots)


def find_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Находит все торговые сигналы:
    1. Одиночная крупная сделка
    2. Кластер крупных сделок
    3. Высокий flow imbalance
    4. Комбинированный сигнал
    """

    signals = []

    for i, row in df.iterrows():
        sig_type = []

        # Сигнал 1: одиночная крупная сделка
        if row["vol_usdt"] >= LARGE_TRADE_USDT:
            sig_type.append("large_trade")

        # Сигнал 2: кластер (много крупных за 3 сек до)
        if row.get(f"large_count_{CLUSTER_WINDOW_SEC}s", 0) >= 2:
            cluster_vol = row.get(f"buy_vol_{CLUSTER_WINDOW_SEC}s", 0) + row.get(f"sell_vol_{CLUSTER_WINDOW_SEC}s", 0)
            if cluster_vol >= CLUSTER_TOTAL_USDT:
                sig_type.append("cluster")

        # Сигнал 3: высокий flow imbalance
        fi_3s = abs(row.get("flow_imb_3s", 0))
        if fi_3s >= FLOW_IMBALANCE_THRESHOLD and row.get("trade_count_3s", 0) >= 5:
            sig_type.append("flow_imbalance")

        if not sig_type:
            continue

        # Определяем направление сигнала
        fi = row.get("flow_imb_3s", 0)
        direction = "BUY" if fi >= 0 else "SELL"

        # Считаем результаты
        result = {
            "ts": row["ts"],
            "price": row["price"],
            "side": row["side"],
            "vol_usdt": row["vol_usdt"],
            "signal_type": "+".join(sig_type),
            "signal_direction": direction,
            "flow_imb_1s": row.get("flow_imb_1s", np.nan),
            "flow_imb_3s": row.get("flow_imb_3s", np.nan),
            "flow_imb_5s": row.get("flow_imb_5s", np.nan),
            "flow_imb_10s": row.get("flow_imb_10s", np.nan),
            "trade_count_3s": row.get("trade_count_3s", 0),
            "large_count_3s": row.get("large_count_3s", 0),
            "buy_vol_3s": row.get("buy_vol_3s", 0),
            "sell_vol_3s": row.get("sell_vol_3s", 0),
        }

        for la in LOOK_AHEAD:
            move = row.get(f"move_{la}s", np.nan)
            result[f"move_{la}s"] = move
            if pd.isna(move):
                result[f"outcome_{la}s"] = None
            elif direction == "BUY":
                result[f"outcome_{la}s"] = (
                    "correct" if move > MIN_IMPULSE_PCT else
                    "wrong" if move < -MIN_IMPULSE_PCT else "flat"
                )
            else:
                result[f"outcome_{la}s"] = (
                    "correct" if move < -MIN_IMPULSE_PCT else
                    "wrong" if move > MIN_IMPULSE_PCT else "flat"
                )

        signals.append(result)

    return pd.DataFrame(signals)


def print_deep_summary(trades: pd.DataFrame, signals: pd.DataFrame, ob: pd.DataFrame) -> str:
    lines = []
    lines.append("=" * 65)
    lines.append("DEEP ANALYSIS SUMMARY")
    lines.append("=" * 65)

    # Общая статистика по сделкам
    lines.append(f"\nПериод: {trades['ts'].min()} → {trades['ts'].max()}")
    lines.append(f"Всего сделок: {len(trades):,}")
    lines.append(f"Крупных сделок (>=${LARGE_TRADE_USDT:,}): {(trades['vol_usdt'] >= LARGE_TRADE_USDT).sum():,}")
    lines.append(f"Сигналов найдено: {len(signals):,}")

    if not signals.empty:
        lines.append(f"\nТипы сигналов:")
        for t, cnt in signals["signal_type"].value_counts().items():
            lines.append(f"  {t}: {cnt}")

        lines.append(f"\n{'─'*65}")
        lines.append("ТОЧНОСТЬ ПО ТАЙМФРЕЙМАМ")
        lines.append(f"{'─'*65}")

        for la in LOOK_AHEAD:
            col = f"outcome_{la}s"
            if col not in signals.columns:
                continue
            valid = signals[signals[col].notna()]
            if valid.empty:
                continue
            c = (valid[col] == "correct").sum()
            w = (valid[col] == "wrong").sum()
            f = (valid[col] == "flat").sum()
            acc = c / (c + w) * 100 if (c + w) > 0 else 0
            avg_move = valid[f"move_{la}s"].abs().mean()
            lines.append(
                f"  {la:>3}s: correct={c:>4} wrong={w:>4} flat={f:>4} | "
                f"точность={acc:>5.1f}% | avg_move={avg_move:.4f}%"
            )

        lines.append(f"\n{'─'*65}")
        lines.append("ЛУЧШИЕ КОМБИНАЦИИ ФИЛЬТРОВ (через 5s)")
        lines.append(f"{'─'*65}")

        col = "outcome_5s"
        for fi_t in [0.5, 0.6, 0.7, 0.8]:
            for min_count in [3, 5, 10]:
                sub = signals[
                    (signals["flow_imb_3s"].abs() >= fi_t) &
                    (signals["trade_count_3s"] >= min_count)
                ]
                if len(sub) < 10:
                    continue
                valid = sub[sub[col].notna()]
                c = (valid[col] == "correct").sum()
                w = (valid[col] == "wrong").sum()
                f = (valid[col] == "flat").sum()
                acc = c / (c + w) * 100 if (c + w) > 0 else 0
                avg_move = valid["move_5s"].abs().mean()
                lines.append(
                    f"  fi>={fi_t} + count>={min_count}: n={len(sub):>5} | "
                    f"acc={acc:>5.1f}% | avg_move={avg_move:.4f}%"
                )

        lines.append(f"\n{'─'*65}")
        lines.append("КОРРЕЛЯЦИИ")
        lines.append(f"{'─'*65}")
        for la in [3, 5, 10]:
            move_col = f"move_{la}s"
            signals["directed_move"] = signals.apply(
                lambda r: r[move_col] if r["signal_direction"] == "BUY" else -r[move_col],
                axis=1
            )
            c1 = signals["vol_usdt"].corr(signals["directed_move"])
            c2 = signals["flow_imb_3s"].corr(signals["directed_move"])
            c3 = signals["trade_count_3s"].corr(signals["directed_move"])
            lines.append(f"  {la}s: corr(vol)={c1:.3f} | corr(flow_imb)={c2:.3f} | corr(count)={c3:.3f}")

    if not ob.empty:
        lines.append(f"\n{'─'*65}")
        lines.append("СТАКАН (текущие снимки)")
        lines.append(f"{'─'*65}")
        lines.append(f"  Средний дисбаланс стакана: {ob['ob_imbalance'].mean():.3f}")
        lines.append(f"  Мин/Макс дисбаланс: {ob['ob_imbalance'].min():.3f} / {ob['ob_imbalance'].max():.3f}")
        lines.append(f"  Средний спред: {ob['spread_pct'].mean():.4f}%")
        lines.append(f"  Средний объём bid top-10: ${ob['bid_vol_top10'].mean():,.0f}")
        lines.append(f"  Средний объём ask top-10: ${ob['ask_vol_top10'].mean():,.0f}")
        lines.append(f"  Крупные bid стены (>$500k): avg {ob['big_bid_count'].mean():.1f} уровней")
        lines.append(f"  Крупные ask стены (>$500k): avg {ob['big_ask_count'].mean():.1f} уровней")

    lines.append(f"\n{'='*65}")
    lines.append("ИТОГОВЫЙ ВЫВОД")
    lines.append(f"{'='*65}")

    if not signals.empty:
        col = "outcome_5s"
        valid = signals[signals[col].notna()]
        if len(valid) > 50:
            c = (valid[col] == "correct").sum()
            w = (valid[col] == "wrong").sum()
            acc = c / (c + w) * 100 if (c + w) > 0 else 0
            avg_move = valid["move_5s"].abs().mean()

            if acc >= 65 and avg_move >= 0.05:
                lines.append(f"✓ СИГНАЛ РАБОТАЕТ: точность {acc:.1f}%, avg движение {avg_move:.4f}%")
                lines.append("  Рекомендуется разработка бота")
            elif acc >= 55:
                lines.append(f"~ СЛАБЫЙ СИГНАЛ: точность {acc:.1f}%")
                lines.append("  Нужны дополнительные фильтры")
            else:
                lines.append(f"✗ СИГНАЛ НЕ РАБОТАЕТ: точность {acc:.1f}%")
        else:
            lines.append("  Недостаточно данных для вывода")

    lines.append("=" * 65)
    return "\n".join(lines)


def main():
    print("BINANCE DEEP ANALYZER v2")
    print("=" * 50)
    print(f"Символ: {SYMBOL} | Период: {HOURS}ч")
    print(f"Порог крупной сделки: ${LARGE_TRADE_USDT:,}")
    print("=" * 50)

    # 1. Сделки
    trades = get_trades(SYMBOL, HOURS)
    trades.to_csv("deep_trades.csv", index=False)
    print(f"  Сохранено: deep_trades.csv")

    # 2. Rolling метрики
    trades = compute_rolling_metrics(trades)

    # 3. Движение цены после каждой сделки
    print("  Считаю движение цены после каждой сделки...")
    trades = compute_price_outcomes(trades)
    trades.to_csv("deep_trades_full.csv", index=False)
    print(f"  Сохранено: deep_trades_full.csv")

    # 4. Стакан
    ob = get_orderbook_snapshots(SYMBOL, HOURS)
    ob.to_csv("deep_orderbook.csv", index=False)
    print(f"  Сохранено: deep_orderbook.csv")

    # 5. Сигналы
    print("  Нахожу сигналы...")
    signals = find_signals(trades)
    signals.to_csv("deep_signals.csv", index=False)
    print(f"  Сигналов найдено: {len(signals):,}")
    print(f"  Сохранено: deep_signals.csv")

    # 6. Итог
    summary = print_deep_summary(trades, signals, ob)
    print("\n" + summary)
    with open("deep_summary.txt", "w") as f:
        f.write(summary)
    print("\nСохранено: deep_summary.txt")


if __name__ == "__main__":
    main()
