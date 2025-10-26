import csv
import hashlib
import json
import tempfile
from pathlib import Path
from typing import List, Dict, Any, Optional, Iterable
from uuid import UUID
from datetime import datetime
from urllib.parse import urlparse
import urllib.request
import glob

import typer
from psycopg.types.json import Json

from ..infrastructure.db import get_conn
from ..shared.logging import setup_logging

app = typer.Typer(add_completion=False)
setup_logging()

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def _is_url(s: str) -> bool:
    try:
        u = urlparse(s)
        return u.scheme in ("http", "https")
    except Exception:
        return False

def _download_to_temp(url: str) -> Path:
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    tmp.close()
    urllib.request.urlretrieve(url, tmp.name)  # nosec - controlled input
    return Path(tmp.name)

def _iter_input_paths(path_or_url: str, glob_pattern: Optional[str]) -> Iterable[Path]:
    if _is_url(path_or_url):
        yield _download_to_temp(path_or_url)
        return
    p = Path(path_or_url)
    if p.is_dir():
        pattern = glob_pattern or "*.csv"
        for f in sorted(p.glob(pattern)):
            if f.is_file():
                yield f
        return
    if any(ch in str(p) for ch in ["*", "?", "["]):  # raw glob in argument
        for f in sorted(Path(".").glob(str(p))):
            if f.is_file():
                yield f
        return
    # single file
    yield p

@app.command("import_events")
def import_events(
    src: str = typer.Argument(..., help="Файл/папка/URL. Напр.: /workspace/events.csv або /workspace/data *.csv або https://..."),
    idempotency_key: Optional[str] = typer.Option(None, "--idempotency-key", "-k", help="Захист від повторного імпорту"),
    batch_size: int = typer.Option(1000, "--batch-size", "-b"),
    glob_pattern: Optional[str] = typer.Option(None, "--glob", help="маска для папки, напр. *.csv"),
):
    """
    Підтримує:
      - локальний файл у контейнері: /workspace/events_sample.csv
      - папку + маску: /workspace/data --glob '*.csv'
      - URL: https://example.com/events.csv
    """
    import asyncio
    asyncio.run(_run_import(src, idempotency_key, batch_size, glob_pattern))

async def _run_import(src: str, idempotency_key: Optional[str], batch_size: int, glob_pattern: Optional[str]):
    conn = await get_conn()
    async with conn.cursor() as cur:
        # перевірка idempotency_key: якщо вже імпортовано — виходимо
        if idempotency_key:
            await cur.execute("SELECT 1 FROM batch_uploads WHERE idempotency_key = %(k)s;", {"k": idempotency_key})
            if await cur.fetchone():
                typer.secho(f"[OK] idempotency_key '{idempotency_key}' already imported. skipping.", fg=typer.colors.GREEN)
                return

        total_inserted = 0
        total_duplicates = 0

        sql = """
            INSERT INTO events (event_id, occurred_at, user_id, event_type, properties)
            VALUES (%(event_id)s, %(occurred_at)s, %(user_id)s, %(event_type)s, %(properties)s)
            ON CONFLICT (event_id) DO NOTHING;
        """

        for path in _iter_input_paths(src, glob_pattern):
            if not path.exists():
                typer.secho(f"[WARN] skip, not found: {path}", fg=typer.colors.YELLOW)
                continue
            checksum = sha256_file(path)
            typer.echo(f"[INFO] reading: {path} (sha256={checksum[:12]}...)")

            inserted = 0
            duplicates = 0
            buf: List[Dict[str, Any]] = []

            async def flush():
                nonlocal inserted, duplicates
                for e in buf:
                    await cur.execute(sql, e)
                    if cur.rowcount and cur.rowcount > 0:
                        inserted += 1
                    else:
                        duplicates += 1
                buf.clear()

            with path.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                required = {"event_id", "occurred_at", "user_id", "event_type", "properties_json"}
                if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
                    raise RuntimeError(f"CSV missing columns; required: {', '.join(sorted(required))}")

                for row in reader:
                    try:
                        event_id = UUID(row["event_id"])
                        occurred_at = datetime.fromisoformat(row["occurred_at"])
                        user_id = str(row["user_id"])
                        event_type = str(row["event_type"])
                        props = json.loads(row["properties_json"] or "{}")
                    except Exception as e:
                        raise RuntimeError(f"Bad row parse: {e}; row={row}") from e

                    buf.append({
                        "event_id": event_id,
                        "occurred_at": occurred_at,
                        "user_id": user_id,
                        "event_type": event_type,
                        "properties": Json(props),
                    })
                    if len(buf) >= batch_size:
                        await flush()

                if buf:
                    await flush()

            total_inserted += inserted
            total_duplicates += duplicates
            typer.secho(f"[DONE] file={path.name} inserted={inserted}, duplicates={duplicates}", fg=typer.colors.GREEN)

        if idempotency_key:
            await cur.execute(
                "INSERT INTO batch_uploads (idempotency_key, file_checksum) VALUES (%(k)s, %(c)s) ON CONFLICT (idempotency_key) DO NOTHING;",
                {"k": idempotency_key, "c": "multi"},  # якщо було кілька файлів, checksum ставимо умовним
            )

        typer.secho(f"[TOTAL] inserted={total_inserted}, duplicates={total_duplicates}", fg=typer.colors.CYAN)
