from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional

from fastapi import APIRouter, Query, HTTPException
from ..infrastructure.db import get_conn
from ..shared.segment import build_segment_filter

router = APIRouter()

@router.get("/stats/dau", summary="Daily Active Users per day in range")
async def stats_dau(
    from_: date = Query(alias="from"),
    to_: date = Query(alias="to"),
    segment: Optional[str] = Query(default=None, description="e.g., event_type:purchase or properties.country=UA"),
):
    if from_ > to_:
        raise HTTPException(status_code=400, detail="'from' must be <= 'to'")

    seg_sql, seg_params = build_segment_filter(segment)
    conn = await get_conn()
    sql = f"""
    WITH dates AS (
        SELECT generate_series(%(from)s::date, %(to)s::date, interval '1 day') AS d
    ),
    agg AS (
        SELECT occurred_at::date AS day, COUNT(DISTINCT user_id) AS dau
        FROM events
        WHERE occurred_at >= %(from)s::date
          AND occurred_at < (%(to)s::date + INTERVAL '1 day')
          {seg_sql}
        GROUP BY occurred_at::date
    )
    SELECT d.d::date AS date, COALESCE(a.dau, 0) AS dau
    FROM dates d
    LEFT JOIN agg a ON a.day = d.d::date
    ORDER BY d.d;
    """
    params = {"from": str(from_), "to": str(to_)}
    params.update(seg_params)

    async with (await get_conn()).cursor() as cur:
        await cur.execute(sql, params)
        rows = await cur.fetchall()

    return [{"date": r["date"].isoformat(), "dau": r["dau"]} for r in rows]

@router.get("/stats/top-events", summary="Top event types in range")
async def stats_top_events(
    from_: date = Query(alias="from"),
    to_: date = Query(alias="to"),
    limit: int = 10,
    segment: Optional[str] = Query(default=None),
):
    if from_ > to_:
        raise HTTPException(status_code=400, detail="'from' must be <= 'to'")
    if not (1 <= limit <= 1000):
        raise HTTPException(status_code=400, detail="limit must be 1..1000")

    seg_sql, seg_params = build_segment_filter(segment)
    sql = f"""
    SELECT event_type, COUNT(*) AS cnt
    FROM events
    WHERE occurred_at >= %(from)s::date
      AND occurred_at < (%(to)s::date + INTERVAL '1 day')
      {seg_sql}
    GROUP BY event_type
    ORDER BY cnt DESC
    LIMIT %(limit)s;
    """
    params = {"from": str(from_), "to": str(to_), "limit": limit}
    params.update(seg_params)

    async with (await get_conn()).cursor() as cur:
        await cur.execute(sql, params)
        rows = await cur.fetchall()

    return [{"event_type": r["event_type"], "count": r["cnt"]} for r in rows]

@router.get("/stats/retention", summary="Simple cohort retention (daily or weekly windows)")
async def stats_retention(
    start_date: date,
    windows: int = 3,
    window_size: str = Query(default="daily", pattern="^(daily|weekly)$"),
    segment: Optional[str] = Query(default=None),
):
    if windows < 1 or windows > 12:
        raise HTTPException(status_code=400, detail="windows must be 1..12")

    # крок в днях
    step_days = 7 if window_size == "weekly" else 1
    seg_sql, seg_params = build_segment_filter(segment)

    sql = f"""
    WITH bounds AS (
        SELECT
            %(start)s::date AS start_date,
            %(step)s::int    AS step_days,
            %(windows)s::int AS windows
    ),
    win AS (
        SELECT
          g AS w,
          (SELECT start_date FROM bounds) + (g * (SELECT step_days FROM bounds)) * INTERVAL '1 day' AS w_start,
          (SELECT start_date FROM bounds) + ((g+1) * (SELECT step_days FROM bounds)) * INTERVAL '1 day' AS w_end
        FROM generate_series(0, (SELECT windows FROM bounds)-1) AS g
    ),
    period AS (
        SELECT (SELECT start_date FROM bounds) AS p_start,
               (SELECT start_date FROM bounds) + ((SELECT windows FROM bounds) * (SELECT step_days FROM bounds)) * INTERVAL '1 day' AS p_end
    ),
    events_f AS (
        SELECT user_id, occurred_at
        FROM events, period
        WHERE occurred_at >= p_start
          AND occurred_at <  p_end
          {seg_sql}
    ),
    first_event AS (
        SELECT user_id, MIN(occurred_at) AS first_at
        FROM events_f GROUP BY user_id
    ),
    cohort AS (
        -- юзери, чия перша подія припала на перше вікно [start, start+step)
        SELECT fe.user_id
        FROM first_event fe, bounds
        WHERE fe.first_at >= (SELECT start_date FROM bounds)
          AND fe.first_at  < (SELECT start_date FROM bounds) + (SELECT step_days FROM bounds) * INTERVAL '1 day'
    ),
    activity AS (
        SELECT w.w AS window, COUNT(DISTINCT e.user_id) AS active
        FROM win w
        JOIN events_f e
          ON e.occurred_at >= w.w_start AND e.occurred_at < w.w_end
        JOIN cohort c ON c.user_id = e.user_id
        GROUP BY w.w
    ),
    cohort_size AS (SELECT COUNT(*) AS size FROM cohort)
    SELECT w.w AS window,
           w.w_start::date AS w_start,
           COALESCE(a.active, 0) AS active,
           (SELECT size FROM cohort_size) AS size
    FROM win w
    LEFT JOIN activity a ON a.window = w.w
    ORDER BY w.w;
    """
    params = {"start": str(start_date), "step": step_days, "windows": windows}
    params.update(seg_params)

    async with (await get_conn()).cursor() as cur:
        await cur.execute(sql, params)
        rows = await cur.fetchall()

    if not rows:
        
        return [{
            "cohort_start_date": start_date.isoformat(),
            "size": 0,
            **{f"w{i}": 0.0 for i in range(windows)},
        }]

    size = rows[0]["size"] or 0
    result = {
        "cohort_start_date": start_date.isoformat(),
        "size": int(size),
    }
    for r in rows:
        w = int(r["window"])
        active = int(r["active"])
        rate = (active / size) if size > 0 else 0.0
        result[f"w{w}"] = round(rate, 4)

    return [result]