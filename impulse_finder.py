"""
Impulse Finder
===============
Находит ВСЕ реальные ценовые импульсы в данных.

Импульс = движение цены > 0.3% за 3 секунды.

Читает clean_trades.csv (уже есть на диске).
Сохраняет impulses.csv с деталями каждого импульса.

Запуск:
  python impulse_finder.py
"""

import pandas as pd
import numpy as np
import time
import os

# ─────────────────────────────────────────
# НАСТРОЙКИ
# ─────────────────────────────────────────

# Порог импульса
MIN_MOVE_PCT    = 0.10   # минимальное движение цены в %
IMPULSE_WINDOW  = 3      # за сколько секунд (3с)

# Cooldown — не считаем продолжение одного импульса как новый
COOLDOWN_SEC    = 30

# Файл с данными
TRADES_FILE = "clean_trades_feb_mar.csv"

# ─────────────────────────────────────────


def load_trades() -> pd.DataFrame:
    print(f"Загружаю {TRADES_FILE} чанками (экономия памяти)...")
    t0 = time.time()

    chunks = []
    total = 0
    for chunk in pd.read_csv(
        TRADES_FILE,
        usecols=["ts", "price", "vol_usdt", "side"],
        dtype={"price": "float32", "vol_usdt": "float32", "side": "str"},
        chunksize=1_000_000
    ):
        chunk["ts"] = pd.to_datetime(chunk["ts"], format="mixed", utc=True)
        chunks.append(chunk)
        total += len(chunk)
        print(f"  Загружено: {total:,}", end="\r")

    df = pd.concat(chunks, ignore_index=True)
    df = df.sort_values("ts").reset_index(drop=True)

    print(f"  {len(df):,} сделок за {time.time()-t0:.1f}с")
    print(f"  Период: {df['ts'].min()} → {df['ts'].max()}")
    return df


def build_second_prices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Строим цену закрытия каждой секунды.
    Это основа для поиска импульсов.
    """
    print("Строю секундные цены...")
    t0 = time.time()

    df["ts_sec"] = df["ts"].dt.floor("1s")

    sec = df.groupby("ts_sec").agg(
        price_open  = ("price", "first"),
        price_close = ("price", "last"),
        price_high  = ("price", "max"),
        price_low   = ("price", "min"),
        vol_total   = ("vol_usdt", "sum"),
        trade_count = ("price", "count"),
    ).reset_index()

    # Заполняем пропуски
    full_range = pd.date_range(
        start=sec["ts_sec"].min(),
        end=sec["ts_sec"].max(),
        freq="1s", tz="UTC"
    )
    sec = sec.set_index("ts_sec").reindex(full_range).reset_index()
    sec = sec.rename(columns={"index": "ts_sec"})
    sec["price_close"] = sec["price_close"].ffill()
    sec["price_open"]  = sec["price_open"].ffill()
    sec["vol_total"]   = sec["vol_total"].fillna(0)
    sec["trade_count"] = sec["trade_count"].fillna(0)

    print(f"  Секундных бакетов: {len(sec):,} за {time.time()-t0:.1f}с")
    return sec


def find_impulses(sec: pd.DataFrame) -> pd.DataFrame:
    """
    Находим все моменты где цена прошла > MIN_MOVE_PCT
    за IMPULSE_WINDOW секунд.

    Логика:
      Для каждой секунды i смотрим:
      price[i + IMPULSE_WINDOW] vs price[i]
      Если разница > MIN_MOVE_PCT → импульс
    """
    print(f"Ищу импульсы > {MIN_MOVE_PCT}% за {IMPULSE_WINDOW}с...")
    t0 = time.time()

    prices = sec["price_close"].values
    n = len(sec)
    w = IMPULSE_WINDOW

    impulses = []
    last_impulse_idx = -COOLDOWN_SEC  # чтобы первый мог сработать

    for i in range(n - w):
        p_start = prices[i]
        p_end   = prices[i + w]

        if p_start <= 0 or np.isnan(p_start) or np.isnan(p_end):
            continue

        move_pct = (p_end - p_start) / p_start * 100

        if abs(move_pct) < MIN_MOVE_PCT:
            continue

        # Cooldown
        if i - last_impulse_idx < COOLDOWN_SEC:
            continue

        direction = "UP" if move_pct > 0 else "DOWN"

        # Детали импульса
        impulses.append({
            "ts":           sec.iloc[i]["ts_sec"],
            "ts_end":       sec.iloc[i + w]["ts_sec"],
            "direction":    direction,
            "price_start":  round(float(p_start), 2),
            "price_end":    round(float(p_end), 2),
            "move_pct":     round(move_pct, 4),
            "move_abs":     round(abs(move_pct), 4),
            # Контекст: объём и сделок за эти 3 секунды
            "vol_during":   sec.iloc[i:i+w]["vol_total"].sum(),
            "trades_during": sec.iloc[i:i+w]["trade_count"].sum(),
            # Контекст: объём за 30 сек ДО
            "vol_30s_before": sec.iloc[max(0,i-30):i]["vol_total"].sum(),
            "trades_30s_before": sec.iloc[max(0,i-30):i]["trade_count"].sum(),
        })

        last_impulse_idx = i

    df_imp = pd.DataFrame(impulses)
    print(f"  Найдено импульсов: {len(df_imp)}")
    print(f"  UP: {(df_imp['direction']=='UP').sum()} | DOWN: {(df_imp['direction']=='DOWN').sum()}")
    print(f"  Время: {time.time()-t0:.1f}с")
    return df_imp


def analyze_impulses(df: pd.DataFrame):
    print()
    print("=" * 60)
    print("АНАЛИЗ ИМПУЛЬСОВ")
    print("=" * 60)
    print(f"Всего: {len(df)}")
    print(f"В сутки: {len(df)/30:.1f}")
    print()

    print("=== РАСПРЕДЕЛЕНИЕ ПО РАЗМЕРУ ===")
    for t in [0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 1.0, 1.5, 2.0]:
        n = (df["move_abs"] >= t).sum()
        print(f"  >= {t:.1f}%: {n:>5} ({n/len(df)*100:.1f}%)")

    print()
    print("=== МЕДИАНА / ПЕРЦЕНТИЛИ РАЗМЕРА ===")
    d = df["move_abs"]
    print(f"  Медиана:  {d.median():.4f}%")
    print(f"  Среднее:  {d.mean():.4f}%")
    print(f"  75%:      {d.quantile(0.75):.4f}%")
    print(f"  90%:      {d.quantile(0.90):.4f}%")
    print(f"  95%:      {d.quantile(0.95):.4f}%")
    print(f"  Макс:     {d.max():.4f}%")

    print()
    print("=== ПО ЧАСАМ СУТОК (UTC) ===")
    df["hour"] = pd.to_datetime(df["ts"]).dt.hour
    by_hour = df.groupby("hour").size()
    for h, cnt in by_hour.items():
        bar = "█" * (cnt // 2)
        print(f"  {h:02d}:00  {cnt:>4}  {bar}")

    print()
    print("=== ТОП 10 СИЛЬНЕЙШИХ ===")
    top = df.nlargest(10, "move_abs")[
        ["ts", "direction", "price_start", "price_end",
         "move_pct", "vol_during", "trades_during"]
    ]
    top["vol_during"] = top["vol_during"].apply(lambda x: f"${x:,.0f}")
    print(top.to_string(index=False))

    print()
    print("=== ОБЪЁМ ВО ВРЕМЯ ИМПУЛЬСА vs ДО ===")
    ratio = df["vol_during"] / (df["vol_30s_before"] / 30 * 3 + 1)
    print(f"  Медианное соотношение vol_during / avg_vol_before: {ratio.median():.2f}x")
    print(f"  (> 1.0 = во время импульса объём выше чем обычно)")
    print(f"  > 2x объём: {(ratio > 2).sum()} импульсов ({(ratio>2).sum()/len(df)*100:.1f}%)")
    print(f"  > 3x объём: {(ratio > 3).sum()} импульсов ({(ratio>3).sum()/len(df)*100:.1f}%)")


def main():
    print("IMPULSE FINDER")
    print("=" * 40)
    print(f"Порог: > {MIN_MOVE_PCT}% за {IMPULSE_WINDOW}с")
    print(f"Cooldown: {COOLDOWN_SEC}с")
    print("=" * 40)

    if not os.path.exists(TRADES_FILE):
        print(f"Файл {TRADES_FILE} не найден!")
        print("Запусти сначала clean_analyzer.py")
        return

    trades  = load_trades()
    sec     = build_second_prices(trades)
    impulses = find_impulses(sec)

    if len(impulses) == 0:
        print("Импульсов не найдено — попробуй снизить MIN_MOVE_PCT")
        return

    analyze_impulses(impulses)
    impulses.to_csv("impulses.csv", index=False)
    print()
    print("Сохранено: impulses.csv")


if __name__ == "__main__":
    main()
