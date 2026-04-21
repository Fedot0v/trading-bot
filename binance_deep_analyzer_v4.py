"""
Binance Deep Analyzer v4 — ASYNC + MULTIPROCESSING
=====================================================
Оптимизации:
  - asyncio + aiohttp для параллельной выгрузки данных (8-10x быстрее)
  - multiprocessing для поиска сигналов по типам параллельно
  - numpy searchsorted для outcomes (O(n log n))
  - pandas resample для rolling метрик (O(n))

Установка:
  pip install aiohttp pandas numpy

Запуск:
  python binance_deep_analyzer_v4.py

Если deep_trades.csv уже есть — выгрузка пропускается автоматически.
"""

import asyncio
import aiohttp
import requests
import pandas as pd
import numpy as np
import time
import os
from datetime import datetime, timezone
from multiprocessing import Pool, cpu_count
from functools import partial

# ─────────────────────────────────────────
# НАСТРОЙКИ
# ─────────────────────────────────────────

SYMBOL                   = "BTCUSDT"
HOURS                    = 48
LARGE_TRADE_USDT         = 200_000
CLUSTER_WINDOW_SEC       = 3
CLUSTER_TOTAL_USDT       = 500_000
FLOW_IMBALANCE_THRESHOLD = 0.6
MIN_IMPULSE_PCT          = 0.05
ROLLING_WINDOWS          = [1, 3, 5, 10]
LOOK_AHEAD               = [2, 3, 5, 10, 15, 30]

# Сколько чанков качать параллельно
ASYNC_CONCURRENCY = 8

BASE_URL = "https://api.binance.com"

# ─────────────────────────────────────────


# ── 1. ASYNC ВЫГРУЗКА ────────────────────

async def fetch_chunk(session: aiohttp.ClientSession,
                      symbol: str,
                      start_ms: int,
                      end_ms: int,
                      semaphore: asyncio.Semaphore) -> list:
    """Выгружает один часовой чанк асинхронно."""

    all_trades = []
    cur = start_ms

    async with semaphore:
        while cur < end_ms:
            params = {
                "symbol": symbol,
                "startTime": cur,
                "endTime": min(cur + 3_600_000, end_ms),
                "limit": 1000,
            }
            for attempt in range(3):
                try:
                    async with session.get(
                        f"{BASE_URL}/api/v3/aggTrades",
                        params=params,
                        timeout=aiohttp.ClientTimeout(total=15),
                    ) as resp:
                        resp.raise_for_status()
                        trades = await resp.json()
                        break
                except Exception as e:
                    if attempt == 2:
                        print(f"\n  Chunk {start_ms} failed: {e}")
                        return all_trades
                    await asyncio.sleep(1)

            if not trades:
                break

            all_trades.extend(trades)
            cur = trades[-1]["T"] + 1
            await asyncio.sleep(0.05)  # мягкий rate limit

    return all_trades


async def get_trades_async(symbol: str, hours: int) -> pd.DataFrame:
    """Параллельно качает все чанки через asyncio."""

    raw_file = "deep_trades.csv"

    if os.path.exists(raw_file):
        print(f"[1/4] {raw_file} уже есть — пропускаю выгрузку")
        df = pd.read_csv(raw_file)
        df["ts"]    = pd.to_datetime(df["ts"], format="mixed", utc=True)
        df["price"] = df["price"].astype(float)
        df["qty"]   = df["qty"].astype(float)
        if "vol_usdt" not in df.columns:
            df["vol_usdt"] = df["price"] * df["qty"]
        else:
            df["vol_usdt"] = df["vol_usdt"].astype(float)
        if "side" not in df.columns:
            df["side"] = df["buyer_maker"].apply(lambda x: "SELL" if x else "BUY")
        if "cvd_delta" not in df.columns:
            df["cvd_delta"] = df.apply(
                lambda r: r["vol_usdt"] if r["side"] == "BUY" else -r["vol_usdt"], axis=1
            )
        print(f"  Загружено: {len(df):,} сделок")
        return df

    end_ms   = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = end_ms - hours * 3600 * 1000

    # Делим на часовые чанки
    chunks = []
    cur = start_ms
    while cur < end_ms:
        chunk_end = min(cur + 3_600_000, end_ms)
        chunks.append((cur, chunk_end))
        cur = chunk_end + 1

    print(f"[1/4] Качаю {len(chunks)} чанков параллельно (concurrency={ASYNC_CONCURRENCY})...")
    t0 = time.time()

    semaphore = asyncio.Semaphore(ASYNC_CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=ASYNC_CONCURRENCY + 4)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            fetch_chunk(session, symbol, s, e, semaphore)
            for s, e in chunks
        ]
        results = await asyncio.gather(*tasks)

    all_trades = [t for chunk in results for t in chunk]
    elapsed = time.time() - t0
    print(f"  Скачано: {len(all_trades):,} сделок за {elapsed:.1f}с")

    df = _parse_trades(all_trades)
    df.to_csv(raw_file, index=False)
    print(f"  Сохранено: {raw_file}")
    return df


def _parse_trades(raw: list) -> pd.DataFrame:
    """Парсит сырые сделки в датафрейм."""
    df = pd.DataFrame(raw).rename(
        columns={"T": "ts_ms", "p": "price", "q": "qty",
                 "m": "buyer_maker", "a": "id"}
    )
    df["ts"]        = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
    df["price"]     = df["price"].astype(float)
    df["qty"]       = df["qty"].astype(float)
    df["vol_usdt"]  = df["price"] * df["qty"]
    df["side"]      = df["buyer_maker"].apply(lambda x: "SELL" if x else "BUY")
    df["cvd_delta"] = df.apply(
        lambda r: r["vol_usdt"] if r["side"] == "BUY" else -r["vol_usdt"], axis=1
    )
    return df.sort_values("ts").reset_index(drop=True)


# ── 2. ROLLING МЕТРИКИ (векторизованно) ──

def compute_rolling_fast(df: pd.DataFrame) -> tuple:
    """Секундные бакеты + pandas rolling — O(n)."""

    print("[2/4] Rolling метрики...")
    t0 = time.time()

    print("  Копирую датафрейм...", end="", flush=True)
    df = df.copy()
    df["ts_sec"] = df["ts"].dt.floor("1s")
    print(" готово")

    # Агрегируем по секундам
    print(f"  Агрегация {len(df):,} сделок по секундам...", end="", flush=True)

    # Быстрая векторизованная агрегация без groupby.apply
    df["buy_vol_item"]  = np.where(df["side"] == "BUY",  df["vol_usdt"], 0)
    df["sell_vol_item"] = np.where(df["side"] == "SELL", df["vol_usdt"], 0)
    df["is_large"]      = (df["vol_usdt"] >= LARGE_TRADE_USDT).astype(int)

    sec = df.groupby("ts_sec", sort=True).agg(
        buy_vol    = ("buy_vol_item",  "sum"),
        sell_vol   = ("sell_vol_item", "sum"),
        count      = ("vol_usdt",      "count"),
        large      = ("is_large",      "sum"),
        cvd_sum    = ("cvd_delta",     "sum"),
        price_last = ("price",         "last"),
    ).reset_index()

    # Убираем временные колонки
    df.drop(columns=["buy_vol_item", "sell_vol_item", "is_large"], inplace=True)

    print(f" {len(sec):,} бакетов ({time.time()-t0:.1f}с)")

    # Заполняем пропущенные секунды
    print("  Заполняю пропущенные секунды...", end="", flush=True)
    full_range = pd.date_range(
        start=sec["ts_sec"].min(),
        end=sec["ts_sec"].max(),
        freq="1s", tz="UTC"
    )
    sec = sec.set_index("ts_sec").reindex(full_range, fill_value=0).reset_index()
    sec = sec.rename(columns={"index": "ts_sec"})
    sec["price_last"] = sec["price_last"].replace(0, np.nan).ffill()
    print(f" {len(sec):,} секунд итого")

    # CVD накопительно
    print("  Считаю CVD...", end="", flush=True)
    sec["cvd_cumulative"] = sec["cvd_sum"].cumsum()
    print(" готово")

    # Rolling по каждому окну
    for i, w in enumerate(ROLLING_WINDOWS, 1):
        print(f"  Rolling окно {w}s ({i}/{len(ROLLING_WINDOWS)})...", end="", flush=True)
        t1 = time.time()
        r = sec["buy_vol"].rolling(w, min_periods=1)
        sec[f"buy_vol_{w}s"]     = r.sum().shift(1)
        sec[f"sell_vol_{w}s"]    = sec["sell_vol"].rolling(w, min_periods=1).sum().shift(1)
        sec[f"trade_count_{w}s"] = sec["count"].rolling(w, min_periods=1).sum().shift(1)
        sec[f"large_count_{w}s"] = sec["large"].rolling(w, min_periods=1).sum().shift(1)
        sec[f"cvd_delta_{w}s"]   = sec["cvd_sum"].rolling(w, min_periods=1).sum().shift(1)
        bv = sec[f"buy_vol_{w}s"].fillna(0)
        sv = sec[f"sell_vol_{w}s"].fillna(0)
        sec[f"flow_imb_{w}s"] = (bv - sv) / (bv + sv + 1)
        print(f" {time.time()-t1:.1f}с")

    # CVD дивергенция: CVD сильно растёт/падает, цена стоит
    print("  Считаю CVD дивергенцию...", end="", flush=True)
    sec["cvd_change_5s"]   = sec["cvd_cumulative"] - sec["cvd_cumulative"].shift(5)
    sec["price_change_5s"] = (
        (sec["price_last"] - sec["price_last"].shift(5))
        / (sec["price_last"].shift(5) + 1) * 100
    )
    sec["cvd_divergence"] = (
        (sec["cvd_change_5s"].abs() > 1_000_000) &
        (sec["price_change_5s"].abs() < 0.03)
    )
    sec["cvd_dir"] = np.where(sec["cvd_change_5s"] > 0, "BUY", "SELL")

    # Мёрджим к сделкам
    print("  Мердж бакетов обратно к сделкам...", end="", flush=True)
    merge_cols = [c for c in sec.columns if c != "ts_sec"]
    df = df.merge(sec[["ts_sec"] + merge_cols], on="ts_sec", how="left")
    print(" готово")

    print(f"  Готово за {time.time()-t0:.1f}с | дивергенций CVD: {sec['cvd_divergence'].sum():,}")
    return df, sec


# ── 3. OUTCOMES (numpy) ───────────────────

def compute_outcomes_fast(df: pd.DataFrame) -> pd.DataFrame:
    """numpy searchsorted — O(n log n)."""

    print("[3/4] Outcomes...")
    t0 = time.time()

    prices = df["price"].values
    times  = df["ts"].values.astype("int64")

    for i, la in enumerate(LOOK_AHEAD, 1):
        print(f"  Оутком {la}s ({i}/{len(LOOK_AHEAD)})...", end="", flush=True)
        t1 = time.time()
        target = times + la * 1_000_000_000
        idx    = np.searchsorted(times, target, side="left")
        idx    = np.clip(idx, 0, len(prices) - 1)
        after  = np.where(idx == np.arange(len(df)), np.nan, prices[idx])
        df[f"price_{la}s"] = after
        df[f"move_{la}s"]  = (after - prices) / prices * 100
        print(f" {time.time()-t1:.2f}с")

    print(f"  Готово за {time.time()-t0:.1f}с")
    return df


# ── 4. СИГНАЛЫ (multiprocessing) ─────────

def _process_signal_type(args) -> pd.DataFrame:
    """Worker для одного типа сигнала — запускается в отдельном процессе."""

    sig_type, df_dict, sec_dict = args
    df  = pd.DataFrame(df_dict)
    sec = pd.DataFrame(sec_dict)

    if sig_type == "large_trade":
        sub = df[df["vol_usdt"] >= LARGE_TRADE_USDT].copy()
        sub["signal_type"]      = "large_trade"
        sub["signal_direction"] = sub["side"]

    elif sig_type == "cluster":
        mask = (
            (df.get(f"large_count_{CLUSTER_WINDOW_SEC}s", pd.Series(0, index=df.index)) >= 2) &
            (
                df.get(f"buy_vol_{CLUSTER_WINDOW_SEC}s", pd.Series(0, index=df.index)) +
                df.get(f"sell_vol_{CLUSTER_WINDOW_SEC}s", pd.Series(0, index=df.index))
                >= CLUSTER_TOTAL_USDT
            )
        )
        sub = df[mask].copy()
        sub["signal_type"]      = "cluster"
        sub["signal_direction"] = sub.get("flow_imb_3s", pd.Series(0, index=sub.index)).apply(
            lambda x: "BUY" if x >= 0 else "SELL"
        )

    elif sig_type == "cvd_divergence":
        div_secs = sec[sec["cvd_divergence"] == True]
        if div_secs.empty:
            return pd.DataFrame()
        sub = df[df["ts_sec"].isin(div_secs["ts_sec"])].copy()
        sub = sub.groupby("ts_sec").first().reset_index()
        sub["signal_type"]      = "cvd_divergence"
        dir_map = dict(zip(div_secs["ts_sec"], div_secs["cvd_dir"]))
        sub["signal_direction"] = sub["ts_sec"].map(dir_map)

    else:
        return pd.DataFrame()

    if sub.empty:
        return pd.DataFrame()

    # Outcomes
    for la in LOOK_AHEAD:
        move_col = f"move_{la}s"
        if move_col not in sub.columns:
            sub[f"outcome_{la}s"] = None
            continue

        def outcome(row, _move_col=move_col, _la=la):
            m = row.get(_move_col)
            if pd.isna(m):
                return None
            d = row["signal_direction"]
            if d == "BUY":
                return "correct" if m >  MIN_IMPULSE_PCT else (
                       "wrong"   if m < -MIN_IMPULSE_PCT else "flat")
            else:
                return "correct" if m < -MIN_IMPULSE_PCT else (
                       "wrong"   if m >  MIN_IMPULSE_PCT else "flat")

        sub[f"outcome_{la}s"] = sub.apply(outcome, axis=1)

    keep = (
        ["ts", "price", "side", "vol_usdt", "signal_type", "signal_direction",
         "flow_imb_1s", "flow_imb_3s", "flow_imb_5s", "flow_imb_10s",
         "trade_count_3s", "large_count_3s", "cvd_delta_5s", "cvd_delta_10s"]
        + [f"move_{la}s"    for la in LOOK_AHEAD]
        + [f"outcome_{la}s" for la in LOOK_AHEAD]
    )
    keep = [c for c in keep if c in sub.columns]
    return sub[keep].drop_duplicates(subset=["ts", "signal_type"])


def find_signals_parallel(df: pd.DataFrame, sec: pd.DataFrame) -> pd.DataFrame:
    """Запускает поиск каждого типа сигнала в отдельном процессе."""

    print("[4/4] Сигналы (multiprocessing)...")
    t0 = time.time()
    print(f"  Сериализую данные для процессов...", end="", flush=True)

    sig_types = ["large_trade", "cluster", "cvd_divergence"]
    df_dict   = df.to_dict("list")
    sec_dict  = sec.to_dict("list")

    print(" готово")
    workers = min(len(sig_types), cpu_count())
    print(f"  Запускаю {workers} процессов параллельно: {sig_types}...", flush=True)
    args    = [(t, df_dict, sec_dict) for t in sig_types]

    with Pool(workers) as pool:
        results = pool.map(_process_signal_type, args)

    signals = pd.concat([r for r in results if not r.empty], ignore_index=True)
    print(f"  Найдено {len(signals):,} сигналов за {time.time()-t0:.1f}с")
    if not signals.empty:
        print(f"  По типам:\n{signals['signal_type'].value_counts().to_string()}")
    return signals


# ── 5. ОТЧЁТ ─────────────────────────────

def print_summary(signals: pd.DataFrame) -> str:
    lines = ["=" * 65, "DEEP ANALYSIS SUMMARY v4", "=" * 65]

    if signals.empty:
        lines.append("Нет сигналов")
        return "\n".join(lines)

    for sig_type in signals["signal_type"].unique():
        sub = signals[signals["signal_type"] == sig_type]
        lines += [f"\n{'─'*65}", f"ТИП: {sig_type.upper()}  (n={len(sub):,})", f"{'─'*65}"]

        for la in LOOK_AHEAD:
            col = f"outcome_{la}s"
            if col not in sub.columns:
                continue
            valid = sub[sub[col].notna()]
            if len(valid) < 5:
                continue
            c   = (valid[col] == "correct").sum()
            w   = (valid[col] == "wrong").sum()
            f   = (valid[col] == "flat").sum()
            acc = c / (c + w) * 100 if (c + w) > 0 else 0
            avg = valid[f"move_{la}s"].abs().mean()
            lines.append(
                f"  {la:>3}s: C={c:>5} W={w:>5} F={f:>5} | "
                f"acc={acc:>5.1f}% | avg={avg:.4f}%"
            )

    lines += [f"\n{'─'*65}", "ЛУЧШИЕ ФИЛЬТРЫ (outcome_5s)", f"{'─'*65}"]
    for fi in [0.5, 0.6, 0.7, 0.8, 0.9]:
        for cnt in [3, 5, 10, 20]:
            sub = signals[
                (signals["flow_imb_3s"].abs() >= fi) &
                (signals["trade_count_3s"] >= cnt)
            ]
            if len(sub) < 20:
                continue
            valid = sub[sub["outcome_5s"].notna()]
            c   = (valid["outcome_5s"] == "correct").sum()
            w   = (valid["outcome_5s"] == "wrong").sum()
            acc = c / (c + w) * 100 if (c + w) > 0 else 0
            avg = valid["move_5s"].abs().mean()
            lines.append(
                f"  fi>={fi} cnt>={cnt:>2}: n={len(sub):>6} | "
                f"acc={acc:>5.1f}% | avg={avg:.4f}%"
            )

    lines += [f"\n{'='*65}", "ИТОГ", f"{'='*65}"]
    best_acc, best_desc = 0, ""
    for la in [2, 3, 5, 10, 15, 30]:
        col   = f"outcome_{la}s"
        if col not in signals.columns:
            continue
        valid = signals[signals[col].notna()]
        if len(valid) < 50:
            continue
        c   = (valid[col] == "correct").sum()
        w   = (valid[col] == "wrong").sum()
        acc = c / (c + w) * 100 if (c + w) > 0 else 0
        avg = valid[f"move_{la}s"].abs().mean()
        if acc > best_acc:
            best_acc  = acc
            best_desc = f"{la}s: acc={acc:.1f}% avg={avg:.4f}%"

    if best_acc >= 65:
        lines.append(f"✓ СИГНАЛ РАБОТАЕТ — {best_desc}")
    elif best_acc >= 55:
        lines.append(f"~ СЛАБЫЙ СИГНАЛ — {best_desc}")
    else:
        lines.append(f"✗ НЕ РАБОТАЕТ — {best_desc}")

    lines.append("=" * 65)
    return "\n".join(lines)


# ── MAIN ──────────────────────────────────

async def async_main():
    print("BINANCE DEEP ANALYZER v4 (ASYNC + MULTIPROCESSING)")
    print("=" * 55)
    t_total = time.time()

    trades = await get_trades_async(SYMBOL, HOURS)
    trades, sec = compute_rolling_fast(trades)
    trades = compute_outcomes_fast(trades)

    print("  Сохраняю deep_trades_full.csv...", end="", flush=True)
    trades.to_csv("deep_trades_full.csv", index=False)
    print(" готово")
    print("  Сохраняю deep_orderbook_sec.csv...", end="", flush=True)
    sec.to_csv("deep_orderbook_sec.csv", index=False)
    print(" готово")

    signals = find_signals_parallel(trades, sec)
    signals.to_csv("deep_signals.csv", index=False)

    summary = print_summary(signals)
    print("\n" + summary)
    with open("deep_summary.txt", "w", encoding="utf-8") as f:
        f.write(summary)

    print(f"\nВсего времени: {time.time()-t_total:.1f}с")
    print("Файлы: deep_trades_full.csv | deep_orderbook_sec.csv | deep_signals.csv | deep_summary.txt")


if __name__ == "__main__":
    asyncio.run(async_main())
