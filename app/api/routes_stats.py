from datetime import date
from typing import List, Dict, Any

from fastapi import APIRouter, Query, HTTPException
from ..infrastructure.db import get_conn

router = APIRouter()

@router.get("/stats/dau", summary="Daily Active Users per day in range")
async def stats_dau(
    from_: date = Query(alias="from"),
    to_: date = Query(alias="to"),
):
    if from_ > to_:
        raise HTTPException(status_code=400, detail="'from' must be <= 'to'")

    conn = await get_conn()
    sql = """
    WITH dates AS (
        SELECT generate_series(%(from)s::date, %(to)s::date, interval '1 day') AS d
    ),
    agg AS (
        SELECT occurred_at::date AS day, COUNT(DISTINCT user_id) AS dau
        FROM events
        WHERE occurred_at >= %(from)s::date
          AND occurred_at < (%(to)s::date + INTERVAL '1 day')
        GROUP BY occurred_at::date
    )
    SELECT d.d::date AS date, COALESCE(a.dau, 0) AS dau
    FROM dates d
    LEFT JOIN agg a ON a.day = d.d::date
    ORDER BY d.d;
    """

    params = {"from": str(from_), "to": str(to_)}

    rows: List[Dict[str, Any]]
    async with conn.cursor() as cur:           # <= тут без await
        await cur.execute(sql, params)
        rows = await cur.fetchall()

    return [{"date": r["date"].isoformat(), "dau": r["dau"]} for r in rows]
