"""
Pre-Impulse Analyzer
=====================
Смотрим что происходило ЗА 5/10/20 секунд ДО каждого импульса.

Читает:
  clean_trades.csv — все сделки
  impulses.csv     — найденные импульсы

Сохраняет:
  pre_impulse.csv  — метрики до каждого импульса

Запуск:
  python pre_impulse_analyzer.py
"""

import pandas as pd
import numpy as np
import time

TRADES_FILE   = "clean_trades_feb_mar.csv"
IMPULSES_FILE = "impulses.csv"
WINDOWS       = [5, 10, 20]  # секунд до импульса
LARGE_TRADE   = 200_000       # порог крупной сделки ($)


def load_data():
    print("Загружаю сделки...")
    t0 = time.time()
    trades = pd.read_csv(
        TRADES_FILE,
        usecols=["ts", "price", "vol_usdt", "side"],
        dtype={"price": "float32", "vol_usdt": "float32", "side": "str"}
    )
    trades["ts"] = pd.to_datetime(trades["ts"], format="mixed", utc=True)
    trades = trades.sort_values("ts").reset_index(drop=True)
    print(f"  {len(trades):,} сделок за {time.time()-t0:.1f}с")

    print("Загружаю импульсы...")
    imp = pd.read_csv(IMPULSES_FILE)
    imp["ts"] = pd.to_datetime(imp["ts"], format="mixed", utc=True)
    print(f"  {len(imp)} импульсов")
    return trades, imp


def build_second_buckets(trades: pd.DataFrame) -> pd.DataFrame:
    """Секундные бакеты с детальными метриками."""
    print("Строю секундные бакеты...")
    t0 = time.time()

    trades["ts_sec"]    = trades["ts"].dt.floor("1s")
    trades["is_buy"]    = (trades["side"] == "BUY").astype("float32")
    trades["is_sell"]   = (trades["side"] == "SELL").astype("float32")
    trades["buy_vol"]   = np.where(trades["side"] == "BUY",  trades["vol_usdt"], 0)
    trades["sell_vol"]  = np.where(trades["side"] == "SELL", trades["vol_usdt"], 0)
    trades["is_large"]  = (trades["vol_usdt"] >= LARGE_TRADE).astype("float32")
    trades["large_vol"] = np.where(trades["vol_usdt"] >= LARGE_TRADE, trades["vol_usdt"], 0)
    trades["cvd"]       = np.where(trades["side"] == "BUY", trades["vol_usdt"], -trades["vol_usdt"])

    sec = trades.groupby("ts_sec").agg(
        price_close  = ("price",     "last"),
        price_open   = ("price",     "first"),
        vol_total    = ("vol_usdt",  "sum"),
        buy_vol      = ("buy_vol",   "sum"),
        sell_vol     = ("sell_vol",  "sum"),
        cvd_sum      = ("cvd",       "sum"),
        trade_count  = ("vol_usdt",  "count"),
        large_count  = ("is_large",  "sum"),
        large_vol    = ("large_vol", "sum"),
        max_trade    = ("vol_usdt",  "max"),
    ).reset_index()

    # Заполняем пропуски
    full_range = pd.date_range(
        start=sec["ts_sec"].min(),
        end=sec["ts_sec"].max(),
        freq="1s", tz="UTC"
    )
    sec = sec.set_index("ts_sec").reindex(full_range, fill_value=0).reset_index()
    sec = sec.rename(columns={"index": "ts_sec"})
    sec["price_close"] = sec["price_close"].replace(0, np.nan).ffill()
    sec["price_open"]  = sec["price_open"].replace(0, np.nan).ffill()

    # Flow imbalance
    sec["flow_imb"] = (sec["buy_vol"] - sec["sell_vol"]) / (sec["vol_total"] + 1)

    # CVD накопительно
    sec["cvd_cum"] = sec["cvd_sum"].cumsum()

    print(f"  {len(sec):,} бакетов за {time.time()-t0:.1f}с")
    return sec


def analyze_before_impulses(imp: pd.DataFrame, sec: pd.DataFrame) -> pd.DataFrame:
    """
    Для каждого импульса считаем метрики за W секунд ДО.
    """
    print("Анализирую данные до каждого импульса...")

    # Индексируем sec по времени для быстрого поиска
    sec_indexed = sec.set_index("ts_sec")

    results = []

    for _, impulse in imp.iterrows():
        imp_ts = impulse["ts"]
        row = {
            "ts":          imp_ts,
            "direction":   impulse["direction"],
            "price_start": impulse["price_start"],
            "move_pct":    impulse["move_pct"],
            "vol_during":  impulse["vol_during"],
            "trades_during": impulse["trades_during"],
        }

        for w in WINDOWS:
            # Окно [imp_ts - w секунд, imp_ts)
            t_from = imp_ts - pd.Timedelta(seconds=w)
            t_to   = imp_ts - pd.Timedelta(seconds=1)

            window = sec_indexed[
                (sec_indexed.index >= t_from) &
                (sec_indexed.index <= t_to)
            ]

            if window.empty:
                for k in _window_keys(w):
                    row[k] = np.nan
                continue

            vol     = window["vol_total"].sum()
            buyvol  = window["buy_vol"].sum()
            sellvol = window["sell_vol"].sum()
            count   = window["trade_count"].sum()
            large   = window["large_count"].sum()
            largev  = window["large_vol"].sum()
            cvd     = window["cvd_sum"].sum()
            maxtr   = window["max_trade"].max()
            fi      = (buyvol - sellvol) / (vol + 1)

            # Изменение цены ЗА это окно
            p_start_w = window.iloc[0]["price_open"]
            p_end_w   = window.iloc[-1]["price_close"]
            price_move_before = (p_end_w - p_start_w) / p_start_w * 100 if p_start_w > 0 else 0

            # Средний объём на секунду (нормированный)
            avg_vol_per_sec = vol / w

            row[f"vol_{w}s"]          = round(vol, 0)
            row[f"buy_vol_{w}s"]      = round(buyvol, 0)
            row[f"sell_vol_{w}s"]     = round(sellvol, 0)
            row[f"cvd_{w}s"]          = round(cvd, 0)
            row[f"flow_imb_{w}s"]     = round(fi, 4)
            row[f"count_{w}s"]        = int(count)
            row[f"large_count_{w}s"]  = int(large)
            row[f"large_vol_{w}s"]    = round(largev, 0)
            row[f"max_trade_{w}s"]    = round(maxtr, 0)
            row[f"price_move_{w}s"]   = round(price_move_before, 4)
            row[f"vol_per_sec_{w}s"]  = round(avg_vol_per_sec, 0)

        results.append(row)

    df = pd.DataFrame(results)
    print(f"  Готово: {len(df)} записей")
    return df


def _window_keys(w):
    return [
        f"vol_{w}s", f"buy_vol_{w}s", f"sell_vol_{w}s",
        f"cvd_{w}s", f"flow_imb_{w}s", f"count_{w}s",
        f"large_count_{w}s", f"large_vol_{w}s", f"max_trade_{w}s",
        f"price_move_{w}s", f"vol_per_sec_{w}s",
    ]


def print_summary(df: pd.DataFrame):
    print()
    print("=" * 65)
    print("ЧТО ПРОИСХОДИТ ДО ИМПУЛЬСА")
    print("=" * 65)

    up   = df[df["direction"] == "UP"]
    down = df[df["direction"] == "DOWN"]

    for label, sub in [("ВСЕ", df), ("UP", up), ("DOWN", down)]:
        if len(sub) == 0:
            continue
        print(f"\n--- {label} ({len(sub)} импульсов) ---")

        for w in WINDOWS:
            print(f"\n  За {w}с ДО:")

            vol_col   = f"vol_{w}s"
            cvd_col   = f"cvd_{w}s"
            fi_col    = f"flow_imb_{w}s"
            lc_col    = f"large_count_{w}s"
            lv_col    = f"large_vol_{w}s"
            pm_col    = f"price_move_{w}s"
            mt_col    = f"max_trade_{w}s"

            print(f"    Объём:          ${sub[vol_col].median():>12,.0f} (медиана)")
            print(f"    CVD:            ${sub[cvd_col].median():>12,.0f} (медиана)")
            print(f"    Flow imbalance: {sub[fi_col].median():>+8.4f} (медиана)")
            print(f"    Крупных сделок: {sub[lc_col].median():>8.1f} (медиана)")
            print(f"    Объём крупных:  ${sub[lv_col].median():>12,.0f}")
            print(f"    Макс сделка:    ${sub[mt_col].median():>12,.0f}")
            print(f"    Движение цены:  {sub[pm_col].median():>+8.4f}% (до импульса)")

    # Сравниваем перед импульсом vs объём во время
    print()
    print("=" * 65)
    print("КЛЮЧЕВОЕ СРАВНЕНИЕ: объём ДО vs ВО ВРЕМЯ импульса")
    print("=" * 65)
    for w in WINDOWS:
        ratio = df["vol_during"] / (df[f"vol_{w}s"] / w * 3 + 1)
        print(f"  vol_during / avg_vol_per_sec_{w}s × 3:")
        print(f"    Медиана: {ratio.median():.1f}x | Мин: {ratio.min():.1f}x | Макс: {ratio.max():.1f}x")

    print()
    print("=" * 65)
    print("КАЖДЫЙ ИМПУЛЬС ДЕТАЛЬНО")
    print("=" * 65)
    cols = ["ts", "direction", "move_pct",
            "flow_imb_5s", "cvd_5s", "large_count_5s",
            "price_move_5s", "vol_5s", "vol_during"]
    print(df[cols].to_string(index=False))


def main():
    print("PRE-IMPULSE ANALYZER")
    print("=" * 40)

    trades, imp = load_data()
    sec = build_second_buckets(trades)
    result = analyze_before_impulses(imp, sec)

    print_summary(result)

    result.to_csv("pre_impulse.csv", index=False)
    print("\nСохранено: pre_impulse.csv")


if __name__ == "__main__":
    main()
