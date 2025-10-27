Архітектура Events Analytics сервісу
1. Context

Це тестове завдання, але цілеспрямовано реалізоване не як тимчасовий скрипт, а як production-ready ingestion analytics-сервіс, який може з часом вирости в платформу на зразок Amplitude / Mixpanel / Segment ingestion layer.
Базові вимоги:

Приймати події батчами (/events, JSON)

Гарантувати ідемпотентність (не можна дублювати)

Оптимізовано для великих CSV імпортів (через CLI)

Prometheus metrics для observability

Можливість масштабування (docker, модульна архітектура)


2. Decision

Було прийнято побудувати сервіс як чистий ingestion microservice на FastAPI + async + Postgres, з такими принципами:

✅ Idempotent ingestion через external_key (-k)
✅ Batch insert з psycopg (async) — не ORM, не SQLAlchemy → чистий контроль за SQL
✅ CLI з Typer → імпорт десятків тисяч подій напряму минаючи HTTP (на проді — через Kafka або S3 ingestion)
✅ Prometheus інтеграція built-in → /metrics готове до Grafana
✅ Docker Compose = reproducible env для тестів / рев’ю / деплою


3. Alternatives considered
Варіант	Чому відмовились
Django + ORM	Занадто важкий моноліт, важко контролювати performance batch ingestion
SQLAlchemy + Alembic	Сповільнює ingestion (ORM overhead), зайві абстракції
Kafka / S3 ingest Складніше сетапити 
JSON зберігати як TEXT	Втрата користі GIN + jsonb_path_ops → аналітика просідає
SQLite	Неможливість показати реальне production use-case
4. Consequences
👍 Позитивні

Production-ready одразу — внутрішнє API, CLI, observability, idempotent write layer

Масштабується горизонтально (можна обгорнути в Kafka / Celery / K8s)

Легко під’єднати BI / Grafana / Feature store

Тестується і бенчмаркається швидко (CLI + Pytest + Metrics)

⚠️ Обмеження / Next Steps

Поки HTTP ingestion не має rate limiting / auth → потрібно буде додати для SaaS

Для retention аналітики зараз SQL мінімальний, але є база для розширення

Якщо навантаження подолає 10k RPS → доведеться додати Kafka / shard / read replicas

✅ Це рішення було прийнято як стратегічно збалансоване між швидкістю розробки (тестове завдання) та продакшн-готовністю.