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

    # retry connect ~30s total
    attempts, delay = 30, 1.0
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
        except Exception as e:
            log.warning("db_connect_failed", attempt=i, error=str(e))
            if i == attempts:
                log.error("db_gave_up_connecting")
                raise
            await asyncio.sleep(delay)
            delay = min(delay * 1.5, 5.0)


async def ensure_migrations() -> None:
    """
    Create tables & indexes idempotently (safe to call on every startup).
    """
    conn = await get_conn()

    stmts = [
        # --- Base events table ---
        """
        CREATE TABLE IF NOT EXISTS events (
            event_id    UUID PRIMARY KEY,
            occurred_at TIMESTAMPTZ NOT NULL,
            user_id     TEXT NOT NULL,
            event_type  TEXT NOT NULL,
            properties  JSONB NOT NULL DEFAULT '{}'::jsonb
        );
        """,
        # --- Indexes ---
        "CREATE INDEX IF NOT EXISTS idx_events_occurred_at     ON events (occurred_at);",
        "CREATE INDEX IF NOT EXISTS idx_events_type_time       ON events (event_type, occurred_at);",
        "CREATE INDEX IF NOT EXISTS idx_events_user_time       ON events (user_id, occurred_at);",
        "CREATE INDEX IF NOT EXISTS idx_events_props_gin       ON events USING GIN (properties jsonb_path_ops);",

        # --- Batch idempotency registry (for CSV import) ---
        """
        CREATE TABLE IF NOT EXISTS batch_uploads (
            idempotency_key TEXT PRIMARY KEY,
            file_checksum   TEXT NOT NULL,
            inserted_at     TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """,
    ]

    async with conn.cursor() as cur:
        for sql in stmts:
            try:
                await cur.execute(sql)
            except Exception as e:
                log.warning(
                    "migration_stmt_failed",
                    error=str(e),
                    sql=sql.strip().splitlines()[0][:120]
                )
        log.info("migrations_applied")


async def shutdown() -> None:
    """Close the global connection if open."""
    global _conn
    if _conn and not _conn.closed:
        await _conn.close()
        _conn = None