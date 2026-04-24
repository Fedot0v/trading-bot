# Торговый бот Polymarket + Binance — Контекст проекта

## Суть проекта

Персональный торговый бот. Читает поток сделок BTC/USDT на Binance в реальном времени, находит ценовые импульсы и автоматически открывает позиции UP/DOWN на платформе Polymarket. Управление через Telegram.

---

## Торговая стратегия

### Сигнал (исследован на 60 днях данных Binance, 29.8M сделок)

За 5 секунд до ценового импульса BTC наблюдается аномальный дисбаланс потока. Это и есть наш сигнал.

**Условия входа (оба одновременно):**
```
flow_imb_5s >= 0.7     # 85%+ объёма идёт в одну сторону за 5с
cvd_5s >= $100,000     # накопленный дисбаланс покупок/продаж
```

**Направление:** flow_imb > 0 → покупаем UP, flow_imb < 0 → покупаем DOWN

**Статистика:**
- Winrate: 69–71% (строгий фильтр)
- Сигналов: 5–6 в сутки
- Медианное движение BTC: 0.15–0.18% за 3 секунды

### Фильтры перед входом
```
vol_30s > 50% нормы       # антифлет — тихий рынок не торгуем
UP/DOWN price < 80¢       # не входим в уже решённый рынок
до конца рынка > 30с      # не входим в последние секунды
```

### Закрытие позиции (логика ТЗ)
```
TP = +25¢ от цены входа → закрыть немедленно
SL = -10¢ от цены входа → закрыть немедленно

Если TP/SL не сработали за 60с:
  → позиция в плюсе: держим до резолюции рынка (100¢)
  → позиция в минусе: закрываем принудительно за 30с до конца

Cooldown: 30с после любого закрытия
Размер ставки: $200 на сделку
```

### P&L симуляция (реальные данные Polymarket, 5.2 часа)
```
TP=25¢ SL=10¢ Timeout=60с:
  Winrate: 55.1%
  Avg/сделка: +$31.26
  5 сигналов/день → месяц: +$4,690
```

---

## Архитектура

### Поток данных
```
Binance WebSocket (aggTrade BTC/USDT)
    ↓ collector/binance.py
Redis pub/sub → market_data:BTCUSDT (MarketData каждую секунду)
    ↓ signal/engine.py
Проверка фильтров → Signal в Redis → signals:cluster
    ↓ trader/polymarket_trader.py
Проверка RiskManager → Polymarket CLOB API (рыночный ордер)
    ↓
PostgreSQL (Trade записан)
    ↓ bot/notifications.py
Telegram уведомление пользователю
```

### Модули
```
shared/
  models.py        — Pydantic v2: MarketData, Signal, Trade, Position, Event, UserSettings
  config.py        — BaseSettings из .env
  redis_bus.py     — типизированный pub/sub
  db.py            — asyncpg + схема
  risk_manager.py  — проверка перед каждой сделкой

collector/
  binance.py       — Binance WS → Redis
  polymarket.py    — CLOB WS → Redis (live цены)

signal/
  filters/         — BaseFilter(ABC), FlowImbalanceFilter, CVDFilter, AntiFlatFilter
  generators/      — BaseSignalGenerator(ABC), ClusterGenerator
  strategies/      — BaseStrategy(ABC), ClusterStrategy
  engine.py        — читает Redis, прогоняет стратегии

trader/
  base.py          — BaseTrader(ABC)
  polymarket_trader.py
  clients/polymarket.py  — aiohttp + eth_account (EIP-712 подписи)

bot/               — aiogram v3, команды + уведомления
infra/             — docker-compose.yml, postgres/init.sql
```

### Стек
```
Python 3.11
asyncio + aiohttp + websockets
redis-py (async pub/sub)
asyncpg (PostgreSQL)
Pydantic v2 (модели и конфиги)
aiogram v3 (Telegram)
eth_account (подпись ордеров Polymarket EIP-712)
Docker Compose (оркестрация)
```

### Redis каналы
```
market_data:{symbol}     → MarketData (каждую секунду)
signals:{strategy_id}    → Signal (при срабатывании)
trades:{user_id}         → события сделок
notifications:{user_id}  → Telegram уведомления
```

### Схема БД (PostgreSQL)
```sql
signals      — id, ts, strategy_id, direction, flow_imb, cvd, btc_price
trades       — id, signal_id, user_id, entry_price, exit_price, pnl, exit_reason, duration_sec
positions    — id, trade_id, user_id, token_id, contracts, entry_price, status
events       — id, ts, type, payload (JSON)  -- аудит лог
user_settings — user_id, tp_cents, sl_cents, stake_usd, active, updated_at
```

---

## Инфраструктура

```
VPS: DigitalOcean Frankfurt, 2GB RAM / 2vCPU, $18/мес
OS:  Ubuntu 24.04 LTS
Git: github.com/FedotOv/trading-bot (private)
```

---

## Паттерны и принципы

### Абстрактные классы
```python
# Trader — есть реализации для разных бирж
class BaseTrader(ABC):
    async def open_position(signal: Signal) -> Trade
    async def close_position(trade: Trade, reason: str) -> Trade
    async def get_balance() -> float

# Strategy — компонентная архитектура
class BaseStrategy(ABC):
    filters: list[BaseFilter]
    generator: BaseSignalGenerator
    async def process(data: MarketData) -> Signal | None

# Filter — каждый делает одно условие
class BaseFilter(ABC):
    async def check(data: MarketData) -> bool
```

### Конкретные реализации
```python
class PolymarketTrader(BaseTrader): ...

class ClusterStrategy(BaseStrategy):
    filters = [
        FlowImbalanceFilter(threshold=0.7),
        CVDFilter(min_cvd=100_000),
        AntiFlatFilter(vol_ratio=0.5),
    ]
    generator = ClusterGenerator()
```

### Режимы торговли
```python
# Через .env
TRADING_MODE=paper  # бумажная торговля
TRADING_MODE=live   # реальная торговля

# Trader выбирается автоматически:
trader = PaperTrader() if settings.trading_mode == "paper" else PolymarketTrader()
```

---

## Polymarket API

Используем напрямую через aiohttp (не SDK):
```
GET  /markets?slug=...          → найти рынок
WS   ws-subscriptions-clob/...  → live цены (CLOB WebSocket)
POST /order                     → создать ордер (требует EIP-712 подпись)
DELETE /order/{id}              → отменить ордер
GET  /positions                 → открытые позиции
```

Авторизация: каждый ордер подписывается приватным ключом Polygon кошелька через eth_account (EIP-712).

---

## Telegram команды
```
/status   — P&L сегодня и за месяц
/trades   — последние 10 сделок
/balance  — баланс USDC на Polymarket
/pause    — пауза торговли
/resume   — возобновить
/settings — изменить TP/SL/размер ставки
/report   — отчёт за период
```

---

## Текущий статус

Завершено:
- Исследование сигнала на 60 днях данных (2 периода × 30 дней)
- Сбор live данных Polymarket и Binance (работает на VPS)
- Симуляция P&L с реальными данными Polymarket
- ТЗ v2 и план разработки

В работе:
- Сбор данных со строгим фильтром (fi>=0.7 + cvd>=$100k) на VPS
- Разработка начинается с shared/models.py (Фаза 1)

Не начато:
- Фазы 1-6 разработки
