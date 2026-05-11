import pandas as pd
import numpy as np
from pathlib import Path

INPUT = "collector_test.csv"

OUT = Path("filter_v3_orderbook_out")
OUT.mkdir(exist_ok=True)

DELAY = 2
FUTURE_WINDOW = 10
TARGET = 0.05

DEDUP_COOLDOWN = 30

WINDOWS = [1, 2, 3, 5]

TRADE_COUNT_TH = [10, 20, 30, 50, 80]
VOL_TH = [10_000, 25_000, 50_000, 100_000, 200_000]

IMB_TH = [0.2, 0.4, 0.6]
IMB_CHANGE_TH = [0.2, 0.4, 0.6]

BOOK_CHANGE_TH = [0.2, 0.4, 0.6]

PRICE_MOVE_TH = [0.005, 0.01, 0.02, 0.03]


def load():
    df = pd.read_csv(INPUT)

    df["sec"] = df["sec"].astype("int64")
    df["price_last"] = df["price_last"].replace(0, np.nan).ffill()

    df = df[df["price_last"].notna()].copy()
    df = df[df["best_bid"] > 0].copy()
    df = df[df["best_ask"] > 0].copy()

    df = df.sort_values("sec").reset_index(drop=True)

    return df


def add_features(df):
    print("[1] Features...")

    df["total_vol"] = df["buy_vol"] + df["sell_vol"]
    df["flow_imb"] = np.where(
        df["total_vol"] > 0,
        (df["buy_vol"] - df["sell_vol"]) / df["total_vol"],
        0.0
    )

    for w in WINDOWS:
        df[f"trade_count_{w}s"] = df["trade_count"].rolling(w, min_periods=1).sum()
        df[f"vol_{w}s"] = df["total_vol"].rolling(w, min_periods=1).sum()
        df[f"buy_vol_{w}s"] = df["buy_vol"].rolling(w, min_periods=1).sum()
        df[f"sell_vol_{w}s"] = df["sell_vol"].rolling(w, min_periods=1).sum()

        total = df[f"buy_vol_{w}s"] + df[f"sell_vol_{w}s"]
        df[f"flow_imb_{w}s"] = np.where(
            total > 0,
            (df[f"buy_vol_{w}s"] - df[f"sell_vol_{w}s"]) / total,
            0.0
        )

        df[f"price_move_{w}s"] = (
            (df["price_last"] - df["price_last"].shift(w))
            / df["price_last"].shift(w)
            * 100
        )

        df[f"imb1_change_{w}s"] = df["imbalance_1"] - df["imbalance_1"].shift(w)
        df[f"imb5_change_{w}s"] = df["imbalance_5"] - df["imbalance_5"].shift(w)

        df[f"bid5_change_{w}s"] = (
            (df["bid_vol_5"] - df["bid_vol_5"].shift(w))
            / df["bid_vol_5"].shift(w).replace(0, np.nan)
        )

        df[f"ask5_change_{w}s"] = (
            (df["ask_vol_5"] - df["ask_vol_5"].shift(w))
            / df["ask_vol_5"].shift(w).replace(0, np.nan)
        )

    return df


def dedup(signals):
    signals = signals.sort_values("sec").reset_index(drop=True)

    kept = []
    last_sec = -10**18

    for row in signals.itertuples(index=False):
        if row.sec - last_sec >= DEDUP_COOLDOWN:
            kept.append(row)
            last_sec = row.sec

    if not kept:
        return pd.DataFrame(columns=signals.columns)

    return pd.DataFrame(kept)


def evaluate(signals, df):
    prices = df["price_last"].to_numpy()
    secs = df["sec"].to_numpy()
    sec_to_idx = {s: i for i, s in enumerate(secs)}

    rows = []

    for _, s in signals.iterrows():
        sec = int(s["sec"])
        direction = s["direction"]

        if sec not in sec_to_idx:
            continue

        i0 = sec_to_idx[sec]
        i_entry = i0 + DELAY

        if i_entry >= len(prices):
            continue

        entry = prices[i_entry]
        future = prices[i_entry:i_entry + FUTURE_WINDOW]

        if len(future) < FUTURE_WINDOW:
            continue

        if direction == "BUY":
            favorable = (future.max() - entry) / entry * 100
            adverse = (entry - future.min()) / entry * 100
        else:
            favorable = (entry - future.min()) / entry * 100
            adverse = (future.max() - entry) / entry * 100

        rows.append({
            "sec": sec,
            "direction": direction,
            "favorable": favorable,
            "adverse": adverse,
            "hit_003": favorable >= 0.03,
            "hit_005": favorable >= 0.05,
            "hit_007": favorable >= 0.07,
            "loss_002": adverse >= 0.02,
            "loss_003": adverse >= 0.03,
        })

    return pd.DataFrame(rows)


def summarize(results, label):
    if results.empty:
        return None

    return {
        "label": label,
        "n": len(results),
        "hit_003": results["hit_003"].mean() * 100,
        "hit_005": results["hit_005"].mean() * 100,
        "hit_007": results["hit_007"].mean() * 100,
        "loss_002": results["loss_002"].mean() * 100,
        "loss_003": results["loss_003"].mean() * 100,
        "median_favorable": results["favorable"].median(),
        "p75_favorable": results["favorable"].quantile(0.75),
        "p90_favorable": results["favorable"].quantile(0.90),
        "median_adverse": results["adverse"].median(),
        "p75_adverse": results["adverse"].quantile(0.75),
        "p90_adverse": results["adverse"].quantile(0.90),
    }


def build_signals(df, w, tc, vol, imb, imb_ch, book_ch, pm):
    # BUY:
    # цена чуть движется вверх
    # поток покупок положительный
    # стакан поддерживает движение:
    # - imbalance растёт
    # - bid-side усиливается ИЛИ ask-side слабеет

    buy = df[
        (df[f"trade_count_{w}s"] >= tc) &
        (df[f"vol_{w}s"] >= vol) &
        (df[f"price_move_{w}s"] >= pm) &
        (df[f"flow_imb_{w}s"] >= imb) &
        (
            (df[f"imb5_change_{w}s"] >= imb_ch) |
            (df[f"bid5_change_{w}s"] >= book_ch) |
            (df[f"ask5_change_{w}s"] <= -book_ch)
        )
    ].copy()

    buy["direction"] = "BUY"

    # SELL зеркально
    sell = df[
        (df[f"trade_count_{w}s"] >= tc) &
        (df[f"vol_{w}s"] >= vol) &
        (df[f"price_move_{w}s"] <= -pm) &
        (df[f"flow_imb_{w}s"] <= -imb) &
        (
            (df[f"imb5_change_{w}s"] <= -imb_ch) |
            (df[f"ask5_change_{w}s"] >= book_ch) |
            (df[f"bid5_change_{w}s"] <= -book_ch)
        )
    ].copy()

    sell["direction"] = "SELL"

    signals = pd.concat([buy, sell], ignore_index=True)

    if signals.empty:
        return signals

    return dedup(signals)


def run_sweep(df):
    print("[2] Sweep filters...")

    summary_rows = []
    all_best_results = []

    counter = 0

    for w in WINDOWS:
        for tc in TRADE_COUNT_TH:
            for vol in VOL_TH:
                for imb in IMB_TH:
                    for imb_ch in IMB_CHANGE_TH:
                        for book_ch in BOOK_CHANGE_TH:
                            for pm in PRICE_MOVE_TH:
                                counter += 1

                                signals = build_signals(
                                    df=df,
                                    w=w,
                                    tc=tc,
                                    vol=vol,
                                    imb=imb,
                                    imb_ch=imb_ch,
                                    book_ch=book_ch,
                                    pm=pm,
                                )

                                if len(signals) < 10:
                                    continue

                                results = evaluate(signals, df)

                                if len(results) < 10:
                                    continue

                                label = (
                                    f"w={w}s_tc>={tc}_vol>={vol}_"
                                    f"imb>={imb}_imbCh>={imb_ch}_"
                                    f"bookCh>={book_ch}_pm>={pm}"
                                )

                                row = summarize(results, label)
                                if row:
                                    summary_rows.append(row)

                                    # сохраняем результаты только перспективных
                                    if (
                                        row["hit_005"] >= 35
                                        and row["median_favorable"] > row["median_adverse"]
                                    ):
                                        r = results.copy()
                                        r["filter_label"] = label
                                        all_best_results.append(r)

    summary = pd.DataFrame(summary_rows)

    if summary.empty:
        print("No valid filters found.")
        return summary

    summary = summary.sort_values(
        ["hit_005", "median_favorable", "loss_002"],
        ascending=[False, False, True]
    )

    summary.to_csv(OUT / "filter_v3_summary.csv", index=False)

    if all_best_results:
        pd.concat(all_best_results, ignore_index=True).to_csv(
            OUT / "filter_v3_best_results.csv",
            index=False
        )

    print("\n=== TOP FILTERS ===")
    print(summary.head(30).to_string(index=False))

    return summary


def main():
    print("[0] Loading...")
    df = load()

    print(f"Rows: {len(df)}")
    print(f"Period seconds: {df['sec'].max() - df['sec'].min()}")

    df = add_features(df)

    summary = run_sweep(df)

    print("\nSaved:")
    print(OUT / "filter_v3_summary.csv")
    print(OUT / "filter_v3_best_results.csv")


if __name__ == "__main__":
    main()