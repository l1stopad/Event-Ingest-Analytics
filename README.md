# Events Analytics — сервіс збору подій та аналітики

Мікросервіс, який приймає події (**ingest**), гарантує **ідемпотентність**, зберігає їх у Postgres, та дозволяє отримувати **продуктову аналітику** (DAU, топ-події, retention).

---

## 🚀 Швидкий запуск

```bash
git clone https://github.com/l1stopad/Event-Ingest-Analytics
cd events-analytics
docker compose up -d
Після запуску доступно:

Сервіс	URL
API (Swagger UI)	http://localhost:8000/docs
PGWeb (UI для Postgres)	http://localhost:8081
Prometheus Metrics	http://localhost:8000/metrics

📡 Prometheus метрики (готові до продакшену)
Ендпоінт /metrics автоматично експонує метрики у форматі Prometheus:

HTTP latency / response time

Error rate (2xx / 4xx / 5xx)

Processing duration per endpoint

Request counter per route

Uptime / process stats

✅ Підходить для Prometheus + Grafana (alerting + dashboards)

📥 Імпорт подій через CLI

docker compose exec api python -m app.cli /data/bench_100k.csv -k bench100k -b 2000
/data/... — шлях усередині контейнера (volume підключено)

-k bench100k — idempotency key (запобігає дублям при повторному імпорті)

-b 2000 — batch size (навантаження → швидкість)

📈 API приклади
Swagger UI → http://localhost:8000/docs

Method	Endpoint	Опис
POST	/events	Інгест батчу подій
GET	/stats/dau?from=2025-08-01&to=2025-08-30	DAU по днях
GET	/stats/top-events?from=...&limit=10	Топ типів подій
GET	/stats/retention?...	Простий когортний retention

🧪 Тести

docker compose exec api pytest -q -o cache_dir=/tmp/.pytest_cache
✅ 5 passed

📊 Benchmark (100k events)
Дані → data/bench_100k.csv

Імпорт
powershell

Measure-Command {
  docker compose exec api python -m app.cli /data/bench_100k.csv -k bench100k -b 2000
} | Select-Object TotalSeconds
→ ~163.5 сек

DAU-запит
powershell
Копировать код
Measure-Command {
  Invoke-WebRequest "http://localhost:8000/stats/dau?from=2025-08-01&to=2025-08-30" > $null
} | Select-Object TotalMilliseconds
→ ~659 мс