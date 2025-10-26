import asyncio
import psycopg
from psycopg.rows import dict_row
import structlog
from ..shared.settings import settings

log = structlog.get_logger()

_conn: psycopg.AsyncConnection | None = None

async def get_conn() -> psycopg.AsyncConnection:
    """Return a singleton async connection with retry on startup."""
    global _conn
    if _conn and not _conn.closed:
        return _conn

    dsn_kwargs = dict(
        host=settings.db_host,
        port=settings.db_port,
        user=settings.db_user,
        password=settings.db_password,
        dbname=settings.db_name,
    )
    attempts, delay = 30, 1.0  # до ~30 секунд очікування

    for i in range(1, attempts + 1):
        try:
            log.info("db_connecting", attempt=i, **dsn_kwargs)
            _conn = await psycopg.AsyncConnection.connect(
                **dsn_kwargs,
                autocommit=True,
                row_factory=dict_row,
            )
            log.info("db_connected")
            return _conn
        except Exception as e:  # noqa: BLE001
            log.warning("db_connect_failed", attempt=i, error=str(e))
            if i == attempts:
                log.error("db_gave_up_connecting")
                raise
            await asyncio.sleep(delay)
            delay = min(delay * 1.5, 5.0)

async def ensure_migrations() -> None:
    # Поки no-op. На Етапі 3 створимо таблиці.
    return None

async def shutdown() -> None:
    global _conn
    if _conn and not _conn.closed:
        await _conn.close()
        _conn = None