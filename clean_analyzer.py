"""
Clean Signal Analyzer
======================
Правильный анализ кластерных сигналов на Binance.

Ключевые отличия от предыдущей версии:
  1. Сигналы ищем на СЕКУНДНЫХ бакетах, не на сделках
     → нет дублей с паузой 1мс
  2. Cooldown между сигналами 30 секунд
     → один импульс = один сигнал
  3. Строгие пороги с самого начала
  4. Честная статистика с перцентилями

Запуск:
  python clean_analyzer.py

Параметры регулируются в НАСТРОЙКАХ ниже.
"""

import requests
import pandas as pd
import numpy as np
import time
import os
from datetime import datetime, timezone

# ─────────────────────────────────────────
# НАСТРОЙКИ
# ─────────────────────────────────────────

SYMBOL = "BTCUSDT"
HOURS  = 720   # 30 дней
# Период: скачиваем другой месяц для валидации
# Удали clean_trades.csv перед запуском чтобы скачать заново

# Пороги сигнала
CVD_THRESHOLD   = 1_000_000   # $1M накопленный дисбаланс за окно
FLOW_IMB_MIN    = 0.6         # перекос потока >= 60%
LARGE_TRADE_USD = 200_000     # порог крупной сделки
LARGE_COUNT_MIN = 2           # минимум крупных сделок за окно

# Окно для расчёта сигнала (секунды)
SIGNAL_WINDOW = 5

# Cooldown между сигналами (секунды)
# Один импульс = один сигнал
COOLDOWN = 30

# Смотрим что случилось через N секунд после сигнала
LOOK_AHEAD = [3, 5, 10, 15, 30, 60]

# Минимальное движение чтобы считать "верным" (%)
MIN_MOVE = 0.05

BASE_URL = "https://api.binance.com"

# ─────────────────────────────────────────


def get_trades(symbol: str, hours: int) -> pd.DataFrame:
    raw_file = "clean_trades_feb_mar.csv"

    if os.path.exists(raw_file):
        print(f"[1/3] {raw_file} уже есть — загружаю...")
        df = pd.read_csv(raw_file)
        df["ts"] = pd.to_datetime(df["ts"], format="mixed", utc=True)
        df["price"]    = df["price"].astype(float)
        df["vol_usdt"] = df["vol_usdt"].astype(float)
        if "cvd_delta" not in df.columns:
            df["cvd_delta"] = df.apply(
                lambda r: r["vol_usdt"] if r["side"]=="BUY" else -r["vol_usdt"], axis=1
            )
        print(f"  Загружено: {len(df):,} сделок")
        return df

    end_ms   = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = end_ms - hours * 3600 * 1000

    end_ms   = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = end_ms - hours * 3600 * 1000

    cur   = start_ms
    batch = 0
    total = 0

    print(f"[1/3] Загружаю {symbol} за {hours}ч ({hours//24} дней)...")
    print(f"  Пишу сразу в {raw_file} (экономия памяти)")

    # Открываем CSV сразу и пишем построчно
    import csv
    headers = ["ts_ms","ts","price","qty","vol_usdt","side","cvd_delta","buyer_maker","id"]

    with open(raw_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        while cur < end_ms:
            chunk_end = min(cur + 3_600_000, end_ms)
            try:
                r = requests.get(
                    f"{BASE_URL}/api/v3/aggTrades",
                    params={"symbol": symbol, "startTime": cur,
                            "endTime": chunk_end, "limit": 1000},
                    timeout=15
                )
                r.raise_for_status()
                trades = r.json()
            except Exception as e:
                print(f"\n  Ошибка: {e} — повтор...")
                time.sleep(2)
                continue

            if not trades:
                cur = chunk_end + 1
                continue

            for t in trades:
                ts_ms    = t["T"]
                price    = float(t["p"])
                qty      = float(t["q"])
                vol_usdt = price * qty
                side     = "SELL" if t["m"] else "BUY"
                cvd      = vol_usdt if side == "BUY" else -vol_usdt
                ts_str   = pd.Timestamp(ts_ms, unit="ms", tz="UTC").isoformat()
                writer.writerow([ts_ms, ts_str, price, qty, vol_usdt, side, cvd, t["m"], t["a"]])

            total += len(trades)
            cur    = trades[-1]["T"] + 1
            batch += 1
            print(f"  {total:,} сделок (батч {batch}) → {raw_file}", end="\r")
            time.sleep(0.12)

    print(f"\n  Итого: {total:,} сделок сохранено в {raw_file}")

    # Теперь читаем обратно для дальнейшей обработки
    print(f"  Загружаю обратно...")
    df = pd.read_csv(raw_file, dtype={"price": "float32", "vol_usdt": "float32", "side": "str"})
    df["ts"] = pd.to_datetime(df["ts"], format="mixed", utc=True)
    df = df.sort_values("ts").reset_index(drop=True)
    print(f"  Готово: {len(df):,} сделок")
    return df


def build_second_buckets(df: pd.DataFrame) -> pd.DataFrame:
    """Агрегируем сделки в секундные бакеты."""

    print("[2/3] Строю секундные бакеты...")
    t0 = time.time()

    df = df.copy()
    df["ts_sec"] = df["ts"].dt.floor("1s")
    df["buy_vol"]  = np.where(df["side"]=="BUY",  df["vol_usdt"], 0)
    df["sell_vol"] = np.where(df["side"]=="SELL", df["vol_usdt"], 0)
    df["is_large"] = (df["vol_usdt"] >= LARGE_TRADE_USD).astype(int)

    sec = df.groupby("ts_sec", sort=True).agg(
        buy_vol    = ("buy_vol",   "sum"),
        sell_vol   = ("sell_vol",  "sum"),
        cvd_sum    = ("cvd_delta", "sum"),
        count      = ("vol_usdt",  "count"),
        large      = ("is_large",  "sum"),
        price_last = ("price",     "last"),
        price_open = ("price",     "first"),
    ).reset_index()

    # Заполняем пропущенные секунды
    full_range = pd.date_range(
        start=sec["ts_sec"].min(),
        end=sec["ts_sec"].max(),
        freq="1s", tz="UTC"
    )
    sec = sec.set_index("ts_sec").reindex(full_range, fill_value=0).reset_index()
    sec = sec.rename(columns={"index": "ts_sec"})
    sec["price_last"] = sec["price_last"].replace(0, np.nan).ffill()
    sec["price_open"] = sec["price_open"].replace(0, np.nan).ffill()

    # Скользящие суммы за SIGNAL_WINDOW секунд
    for col in ["buy_vol", "sell_vol", "cvd_sum", "count", "large"]:
        sec[f"{col}_w"] = sec[col].rolling(SIGNAL_WINDOW, min_periods=1).sum().shift(1)

    # Имбаланс потока
    bv = sec["buy_vol_w"]
    sv = sec["sell_vol_w"]
    sec["flow_imb"] = (bv - sv) / (bv + sv + 1)

    # CVD накопительно (для справки)
    sec["cvd_cum"] = sec["cvd_sum"].cumsum()

    print(f"  Бакетов: {len(sec):,} за {time.time()-t0:.1f}с")
    return sec


def find_clean_signals(sec: pd.DataFrame) -> pd.DataFrame:
    """
    Ищем сигналы на секундных бакетах с cooldown.
    Один импульс = один сигнал.
    """

    print("[3/3] Ищу сигналы...")

    prices = sec["price_last"].values
    ts     = sec["ts_sec"].values
    n      = len(sec)

    signals = []
    last_signal_ts = None

    for i in range(SIGNAL_WINDOW, n - max(LOOK_AHEAD)):

        # Фильтр 1: крупные сделки
        if sec.iloc[i]["large_w"] < LARGE_COUNT_MIN:
            continue

        # Фильтр 2: CVD
        cvd = abs(sec.iloc[i]["cvd_sum_w"])
        if cvd < CVD_THRESHOLD:
            continue

        # Фильтр 3: flow imbalance
        fi = abs(sec.iloc[i]["flow_imb"])
        if fi < FLOW_IMB_MIN:
            continue

        # Cooldown — не берём сигнал если недавно был другой
        cur_ts = ts[i]
        if last_signal_ts is not None:
            gap = (pd.Timestamp(cur_ts) - pd.Timestamp(last_signal_ts)).total_seconds()
            if gap < COOLDOWN:
                continue

        # Направление
        direction = "BUY" if sec.iloc[i]["flow_imb"] > 0 else "SELL"

        # Цена сигнала
        price_entry = prices[i]
        if price_entry == 0 or np.isnan(price_entry):
            continue

        # Движение цены через N секунд
        row = {
            "ts":         cur_ts,
            "direction":  direction,
            "price":      price_entry,
            "flow_imb":   sec.iloc[i]["flow_imb"],
            "cvd_5s":     sec.iloc[i]["cvd_sum_w"],
            "large_count": sec.iloc[i]["large_w"],
            "trade_count": sec.iloc[i]["count_w"],
        }

        # Ищем цену через LOOK_AHEAD секунд
        for la in LOOK_AHEAD:
            target_idx = i + la
            if target_idx < n:
                p_after = prices[target_idx]
                if p_after > 0 and not np.isnan(p_after):
                    move = (p_after - price_entry) / price_entry * 100
                    row[f"move_{la}s"] = round(move, 6)
                    # Направленное движение
                    directed = move if direction == "BUY" else -move
                    row[f"directed_{la}s"] = round(directed, 6)
                    row[f"correct_{la}s"] = (
                        "correct" if directed > MIN_MOVE else
                        "wrong"   if directed < -MIN_MOVE else "flat"
                    )

        signals.append(row)
        last_signal_ts = cur_ts

    df_sig = pd.DataFrame(signals)
    print(f"  Найдено сигналов: {len(df_sig)}")
    print(f"  BUY: {(df_sig['direction']=='BUY').sum()} | SELL: {(df_sig['direction']=='SELL').sum()}")
    return df_sig


def analyze(signals: pd.DataFrame):
    """Честная статистика."""

    print("\n" + "="*65)
    print("РЕЗУЛЬТАТЫ")
    print("="*65)
    print(f"Сигналов всего: {len(signals)}")
    print(f"За {HOURS//24} дней = {len(signals)/(HOURS/24):.1f} в сутки")
    print(f"Cooldown между сигналами: {COOLDOWN}с")
    print(f"CVD порог: ${CVD_THRESHOLD:,.0f} | Flow imb: {FLOW_IMB_MIN}")
    print()

    for la in LOOK_AHEAD:
        col = f"directed_{la}s"
        cor = f"correct_{la}s"
        if col not in signals.columns:
            continue

        d = signals[col].dropna()
        c_col = signals[cor].dropna()
        c = (c_col == "correct").sum()
        w = (c_col == "wrong").sum()
        f = (c_col == "flat").sum()
        acc = c/(c+w)*100 if (c+w) > 0 else 0

        print(f"Через {la:>3}с:")
        print(f"  Winrate:  {c}/{c+w} = {acc:.1f}%")
        print(f"  Медиана:  {d.median():+.4f}%")
        print(f"  Среднее:  {d.mean():+.4f}%")
        print(f"  25%/75%:  {d.quantile(0.25):+.4f}% / {d.quantile(0.75):+.4f}%")
        print(f"  90%/95%:  {d.quantile(0.90):+.4f}% / {d.quantile(0.95):+.4f}%")
        print(f"  > 0.2%:   {(d>0.2).sum()} ({(d>0.2).sum()/len(d)*100:.1f}%)")
        print(f"  > 0.3%:   {(d>0.3).sum()} ({(d>0.3).sum()/len(d)*100:.1f}%)")
        print(f"  > 0.5%:   {(d>0.5).sum()} ({(d>0.5).sum()/len(d)*100:.1f}%)")
        print()

    # Лучшие фильтры
    print("─"*65)
    print("ЛУЧШИЕ ДОПОЛНИТЕЛЬНЫЕ ФИЛЬТРЫ (через 5с)")
    print("─"*65)

    la = 5
    dcol = f"directed_{la}s"
    ccol = f"correct_{la}s"

    if dcol in signals.columns:
        print(f"{'CVD':>8} {'fi':>5} {'n':>5} {'win%':>6} {'med':>8} {'p75':>7} {'p90':>7}")
        print("-"*55)

        for cvd_t in [CVD_THRESHOLD, CVD_THRESHOLD*2, CVD_THRESHOLD*3, CVD_THRESHOLD*5]:
            for fi_t in [FLOW_IMB_MIN, 0.7, 0.8, 0.9]:
                sub = signals[
                    (signals["cvd_5s"].abs() >= cvd_t) &
                    (signals["flow_imb"].abs() >= fi_t)
                ]
                if len(sub) < 5:
                    continue
                d = sub[dcol].dropna()
                c = (sub[ccol] == "correct").sum()
                w = (sub[ccol] == "wrong").sum()
                if c+w == 0: continue
                acc = c/(c+w)*100
                med = d.median()
                p75 = d.quantile(0.75)
                p90 = d.quantile(0.90)
                mark = " ★" if acc >= 65 and med >= 0.15 else ""
                print(f"  {cvd_t/1e6:.1f}M  {fi_t:.1f}  {len(sub):>4}  "
                      f"{acc:>5.1f}%  {med:>+7.4f}%  {p75:>+6.4f}%  {p90:>+6.4f}%{mark}")

    print("="*65)
    signals.to_csv("clean_signals.csv", index=False)
    print("Сохранено: clean_signals.csv")


def main():
    print("CLEAN SIGNAL ANALYZER")
    print("="*40)
    print(f"Период: {HOURS}ч ({HOURS//24} дней)")
    print(f"CVD >= ${CVD_THRESHOLD:,} | Flow >= {FLOW_IMB_MIN}")
    print(f"Cooldown: {COOLDOWN}с")
    print("="*40)

    trades  = get_trades(SYMBOL, HOURS)
    sec     = build_second_buckets(trades)
    signals = find_clean_signals(sec)

    if len(signals) > 0:
        analyze(signals)
    else:
        print("Сигналов не найдено — попробуй снизить пороги")


if __name__ == "__main__":
    main()
