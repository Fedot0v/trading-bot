"""
Binance BTC Trade Flow Analyzer
================================
Выгружает исторические сделки по BTC/USDT с Binance
и анализирует: предшествует ли крупная сделка импульсу цены.

Запуск:
  pip install requests pandas
  python binance_analyzer.py

Результат:
  - binance_trades_raw.csv     — все сделки за период
  - binance_signals.csv        — моменты с крупными сделками + что было после
  - binance_summary.txt        — итоговая статистика
"""

import requests
import pandas as pd
import time
from datetime import datetime, timezone

# ─────────────────────────────────────────
# НАСТРОЙКИ — меняй здесь
# ─────────────────────────────────────────

SYMBOL = "BTCUSDT"

# Сколько часов истории выгружать (макс 24)
HOURS = 6

# Порог крупной сделки в USDT
# Сделка считается "крупной" если объём >= этого значения
LARGE_TRADE_THRESHOLD_USDT = 200_000  # $200k

# Через сколько секунд смотреть на изменение цены после крупной сделки
LOOK_AHEAD_SECONDS = [2, 3, 5, 10]

# Минимальный импульс цены который считаем "значимым" (в %)
MIN_PRICE_MOVE_PCT = 0.05  # 0.05%

# ─────────────────────────────────────────

BASE_URL = "https://api.binance.com"


def get_recent_trades(symbol: str, hours: int) -> pd.DataFrame:
    """Выгружает агрегированные сделки за последние N часов."""
    
    end_time = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_time = end_time - (hours * 3600 * 1000)
    
    all_trades = []
    current_start = start_time
    
    print(f"Выгружаю сделки {symbol} за последние {hours} часов...")
    print(f"Порог крупной сделки: ${LARGE_TRADE_THRESHOLD_USDT:,}")
    print("-" * 50)
    
    request_count = 0
    
    while current_start < end_time:
        url = f"{BASE_URL}/api/v3/aggTrades"
        params = {
            "symbol": symbol,
            "startTime": current_start,
            "endTime": min(current_start + 3600_000, end_time),  # по 1 часу
            "limit": 1000
        }
        
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            trades = resp.json()
        except Exception as e:
            print(f"Ошибка запроса: {e}")
            time.sleep(1)
            continue
        
        if not trades:
            current_start += 3600_000
            continue
        
        all_trades.extend(trades)
        current_start = trades[-1]["T"] + 1
        request_count += 1
        
        print(f"  Загружено сделок: {len(all_trades):,} | Запросов: {request_count}", end="\r")
        
        # Лимит Binance API — не более 20 запросов в секунду
        time.sleep(0.1)
    
    print(f"\nВсего загружено сделок: {len(all_trades):,}")
    
    if not all_trades:
        print("Нет данных!")
        return pd.DataFrame()
    
    df = pd.DataFrame(all_trades)
    df = df.rename(columns={
        "T": "timestamp_ms",
        "p": "price",
        "q": "qty",
        "m": "is_buyer_maker",
        "a": "agg_trade_id"
    })
    
    df["timestamp"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
    df["price"] = df["price"].astype(float)
    df["qty"] = df["qty"].astype(float)
    df["volume_usdt"] = df["price"] * df["qty"]
    
    # is_buyer_maker=True означает что покупатель был мейкером
    # то есть продавец был агрессором (тейкером)
    # Нас интересует агрессивная сторона:
    df["side"] = df["is_buyer_maker"].apply(
        lambda x: "SELL" if x else "BUY"  # тейкер
    )
    
    return df.sort_values("timestamp").reset_index(drop=True)


def analyze_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Находит крупные сделки и смотрит что происходит с ценой после.
    """
    
    if df.empty:
        return pd.DataFrame()
    
    large_trades = df[df["volume_usdt"] >= LARGE_TRADE_THRESHOLD_USDT].copy()
    
    print(f"\nКрупных сделок (>= ${LARGE_TRADE_THRESHOLD_USDT:,}): {len(large_trades):,}")
    print(f"Из них BUY: {(large_trades['side'] == 'BUY').sum()}")
    print(f"Из них SELL: {(large_trades['side'] == 'SELL').sum()}")
    
    results = []
    
    for _, trade in large_trades.iterrows():
        trade_time = trade["timestamp"]
        trade_price = trade["price"]
        trade_side = trade["side"]
        trade_vol = trade["volume_usdt"]
        
        row = {
            "time": trade_time,
            "price_at_signal": trade_price,
            "side": trade_side,
            "volume_usdt": trade_vol,
        }
        
        # Смотрим что было ДО сделки (контекст за 5 секунд)
        window_before = df[
            (df["timestamp"] >= trade_time - pd.Timedelta(seconds=5)) &
            (df["timestamp"] < trade_time)
        ]
        
        if not window_before.empty:
            buy_vol_before = window_before[window_before["side"] == "BUY"]["volume_usdt"].sum()
            sell_vol_before = window_before[window_before["side"] == "SELL"]["volume_usdt"].sum()
            row["buy_vol_5s_before"] = buy_vol_before
            row["sell_vol_5s_before"] = sell_vol_before
            row["flow_bias_before"] = (buy_vol_before - sell_vol_before) / (buy_vol_before + sell_vol_before + 1)
        else:
            row["buy_vol_5s_before"] = 0
            row["sell_vol_5s_before"] = 0
            row["flow_bias_before"] = 0
        
        # Смотрим что происходит ПОСЛЕ через N секунд
        for seconds in LOOK_AHEAD_SECONDS:
            window_after = df[
                (df["timestamp"] > trade_time) &
                (df["timestamp"] <= trade_time + pd.Timedelta(seconds=seconds))
            ]
            
            if window_after.empty:
                row[f"price_after_{seconds}s"] = None
                row[f"move_pct_{seconds}s"] = None
                row[f"direction_{seconds}s"] = None
                continue
            
            price_after = window_after.iloc[-1]["price"]
            move_pct = (price_after - trade_price) / trade_price * 100
            
            row[f"price_after_{seconds}s"] = price_after
            row[f"move_pct_{seconds}s"] = round(move_pct, 4)
            
            # Совпало ли направление с нашей крупной сделкой?
            if trade_side == "BUY":
                row[f"direction_{seconds}s"] = "correct" if move_pct > MIN_PRICE_MOVE_PCT else (
                    "wrong" if move_pct < -MIN_PRICE_MOVE_PCT else "flat"
                )
            else:  # SELL
                row[f"direction_{seconds}s"] = "correct" if move_pct < -MIN_PRICE_MOVE_PCT else (
                    "wrong" if move_pct > MIN_PRICE_MOVE_PCT else "flat"
                )
        
        results.append(row)
    
    return pd.DataFrame(results)


def print_summary(signals: pd.DataFrame) -> str:
    """Выводит итоговую статистику."""
    
    lines = []
    lines.append("=" * 60)
    lines.append("ИТОГОВАЯ СТАТИСТИКА")
    lines.append("=" * 60)
    lines.append(f"Всего крупных сделок: {len(signals)}")
    lines.append("")
    
    for seconds in LOOK_AHEAD_SECONDS:
        col = f"direction_{seconds}s"
        move_col = f"move_pct_{seconds}s"
        
        if col not in signals.columns:
            continue
        
        valid = signals[signals[col].notna()]
        if valid.empty:
            continue
        
        correct = (valid[col] == "correct").sum()
        wrong = (valid[col] == "wrong").sum()
        flat = (valid[col] == "flat").sum()
        total = len(valid)
        
        accuracy = correct / (correct + wrong) * 100 if (correct + wrong) > 0 else 0
        avg_move = valid[move_col].abs().mean()
        
        lines.append(f"Через {seconds} секунд после крупной сделки:")
        lines.append(f"  Верное направление:  {correct}/{total} ({accuracy:.1f}%)")
        lines.append(f"  Неверное направление: {wrong}/{total}")
        lines.append(f"  Плоско (< {MIN_PRICE_MOVE_PCT}%): {flat}/{total}")
        lines.append(f"  Среднее движение цены: {avg_move:.4f}%")
        lines.append("")
    
    # Анализ flow bias
    lines.append("-" * 40)
    lines.append("АНАЛИЗ КОНТЕКСТА (за 5 сек до крупной сделки):")
    
    buy_signals = signals[signals["side"] == "BUY"]
    sell_signals = signals[signals["side"] == "SELL"]
    
    if not buy_signals.empty:
        avg_bias_buy = buy_signals["flow_bias_before"].mean()
        lines.append(f"  Крупные BUY — средний flow bias до: {avg_bias_buy:.3f}")
        lines.append(f"  (1.0 = все покупали, -1.0 = все продавали)")
    
    if not sell_signals.empty:
        avg_bias_sell = sell_signals["flow_bias_before"].mean()
        lines.append(f"  Крупные SELL — средний flow bias до: {avg_bias_sell:.3f}")
    
    lines.append("")
    lines.append("=" * 60)
    lines.append("ВЫВОД ДЛЯ СТРАТЕГИИ:")
    lines.append("")
    
    # Автоматический вывод
    col_3s = "direction_3s"
    if col_3s in signals.columns:
        valid = signals[signals[col_3s].notna()]
        correct = (valid[col_3s] == "correct").sum()
        wrong = (valid[col_3s] == "wrong").sum()
        accuracy_3s = correct / (correct + wrong) * 100 if (correct + wrong) > 0 else 0
        
        if accuracy_3s >= 65:
            lines.append(f"✓ Через 3 секунды точность {accuracy_3s:.1f}% — СИГНАЛ РАБОТАЕТ")
            lines.append("  Стратегия имеет смысл для дальнейшей разработки")
        elif accuracy_3s >= 55:
            lines.append(f"~ Через 3 секунды точность {accuracy_3s:.1f}% — СЛАБЫЙ СИГНАЛ")
            lines.append("  Нужно добавить фильтры (объём, время суток, flow bias)")
        else:
            lines.append(f"✗ Через 3 секунды точность {accuracy_3s:.1f}% — СИГНАЛ НЕ РАБОТАЕТ")
            lines.append("  Крупные сделки не предсказывают направление движения")
    
    lines.append("=" * 60)
    
    summary = "\n".join(lines)
    print(summary)
    return summary


def main():
    print("BINANCE TRADE FLOW ANALYZER")
    print("=" * 50)
    
    # 1. Выгружаем сделки
    df = get_recent_trades(SYMBOL, HOURS)
    
    if df.empty:
        print("Не удалось выгрузить данные")
        return
    
    # 2. Сохраняем сырые данные
    raw_file = "binance_trades_raw.csv"
    df.to_csv(raw_file, index=False)
    print(f"Сырые данные сохранены: {raw_file}")
    print(f"Период: {df['timestamp'].min()} → {df['timestamp'].max()}")
    print(f"Всего сделок: {len(df):,}")
    print(f"Мин цена: ${df['price'].min():,.2f}")
    print(f"Макс цена: ${df['price'].max():,.2f}")
    
    # 3. Анализируем сигналы
    signals = analyze_signals(df)
    
    if signals.empty:
        print("Крупных сделок не найдено. Попробуй снизить LARGE_TRADE_THRESHOLD_USDT")
        return
    
    # 4. Сохраняем сигналы
    signals_file = "binance_signals.csv"
    signals.to_csv(signals_file, index=False)
    print(f"Сигналы сохранены: {signals_file}")
    
    # 5. Итоговая статистика
    summary = print_summary(signals)
    
    with open("binance_summary.txt", "w") as f:
        f.write(summary)
    print("\nСводка сохранена: binance_summary.txt")


if __name__ == "__main__":
    main()
