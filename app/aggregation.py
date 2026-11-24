import json
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from app.common import BUCKET, ch, ch_rows, log, pg, retry, s3

cycle_lock = threading.Lock()
stop = threading.Event()
INTERVAL = int(os.getenv("AGGREGATION_INTERVAL_SECONDS", "300"))
if INTERVAL <= 0:
    raise ValueError("AGGREGATION_INTERVAL_SECONDS должен быть положительным")


def today():
    return datetime.now(timezone.utc).date()


def calculate(day):
    started = time.monotonic()
    log.info("Начат цикл агрегации date=%s", day)
    # day is a validated Python date, never an arbitrary SQL fragment.
    day_sql = f"toDate('{day.isoformat()}')"
    daily = ch_rows(f"""
        SELECT count() AS records, uniqExact(user_id) AS dau,
               avgOrNullIf(progress_seconds, event_type = 'VIEW_FINISHED') AS avg_watch_seconds,
               countIf(event_type = 'VIEW_FINISHED') /
                   nullIf(countIf(event_type = 'VIEW_STARTED'), 0) AS conversion
        FROM raw_events FINAL WHERE toDate(timestamp) = {day_sql}
    """)[0]
    movies = ch_rows(f"""
        SELECT movie_id, sum(event_type = 'VIEW_STARTED') AS views
        FROM raw_events FINAL WHERE toDate(timestamp) = {day_sql}
        GROUP BY movie_id HAVING views > 0 ORDER BY views DESC, movie_id
    """)
    cohort_cte = f"""
        WITH first_views AS (
            SELECT user_id, min(toDate(timestamp)) AS first_day
            FROM raw_events FINAL WHERE event_type = 'VIEW_STARTED' GROUP BY user_id
        ), cohort_users AS (
            SELECT user_id FROM first_views WHERE first_day = {day_sql}
        )
    """
    cohort_size = int(ch_rows(cohort_cte + " SELECT count() AS n FROM cohort_users")[0]["n"])
    retention = ch_rows(cohort_cte + f"""
        SELECT dateDiff('day', {day_sql}, toDate(timestamp)) AS age,
               uniqExact(user_id) / nullIf({cohort_size}, 0) AS retention
        FROM raw_events FINAL
        WHERE user_id IN (SELECT user_id FROM cohort_users)
          AND toDate(timestamp) BETWEEN {day_sql} AND addDays({day_sql}, 7)
        GROUP BY age
    """)
    by_age = {int(row["age"]): row["retention"] for row in retention}
    computed = datetime.now(timezone.utc)
    result = []

    def add(name, value, dimension=""):
        result.append({"metric_date": day.isoformat(), "metric_name": name,
                       "dimension": dimension, "value": None if value is None else float(value),
                       "computed_at": computed.strftime("%Y-%m-%d %H:%M:%S.%f")})

    for metric in ("dau", "avg_watch_seconds", "conversion"):
        add(metric, daily[metric])
    for row in movies:
        add("movie_views", row["views"], row["movie_id"])
    for age in range(8):
        matured = day + timedelta(days=age) <= today()
        add(f"retention_d{age}", by_age.get(age, 0) if cohort_size and matured else None)

    # CH stores computed results, Grafana reads the latest row per key via FINAL.
    ch("INSERT INTO daily_metrics", result)

    def save_postgres():
        with pg() as conn:
            with conn.cursor() as cursor:
                cursor.executemany("""
                    INSERT INTO daily_metrics (metric_date, metric_name, dimension, value, computed_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (metric_date, metric_name, dimension) DO UPDATE
                    SET value = EXCLUDED.value, computed_at = EXCLUDED.computed_at
                """, [(day, row["metric_name"], row["dimension"], row["value"], computed) for row in result])

    retry(save_postgres)
    elapsed = round(time.monotonic() - started, 3)
    log.info("Цикл завершён: date=%s, обработано %s записей за %s сек, метрик=%s",
             day, daily["records"], elapsed, len(result))
    return {"date": day.isoformat(), "records": int(daily["records"]), "metrics": len(result), "seconds": elapsed}


def export_day(day):
    try:
        with pg() as conn:
            rows = conn.execute("""
                SELECT metric_date, metric_name, dimension, value, computed_at
                FROM daily_metrics WHERE metric_date = %s ORDER BY metric_name, dimension
            """, (day,)).fetchall()
        if not rows:
            raise LookupError("За указанную дату нет рассчитанных метрик")
        contents = [{"date": row[0].isoformat(), "metric_name": row[1], "dimension": row[2],
                     "value": row[3], "computed_at": row[4].astimezone(timezone.utc).isoformat()} for row in rows]
        key = f"daily/{day.isoformat()}/aggregates.json"
        s3().put_object(Bucket=BUCKET, Key=key,
                        Body=json.dumps(contents, ensure_ascii=False, allow_nan=False).encode(),
                        ContentType="application/json")
        log.info("Экспорт date=%s metrics=%s s3://%s/%s", day, len(rows), BUCKET, key)
        return {"date": day.isoformat(), "metrics": len(rows), "object": f"s3://{BUCKET}/{key}"}
    except Exception:
        log.exception("Ошибка экспорта date=%s; плановый запуск повторит экспорт", day)
        raise


def schedule():
    pending_exports = set()
    while not stop.is_set():
        pending_exports.add(today() - timedelta(days=1))
        try:
            with cycle_lock:
                # Revisit older cohorts, so D1/D7 and late-arriving events get refreshed.
                days = {date.fromisoformat(row["day"]) for row in ch_rows(
                    "SELECT DISTINCT toDate(timestamp) AS day FROM raw_events FINAL WHERE toDate(timestamp) <= toDate(now('UTC'))")}
                days.update({today(), today() - timedelta(days=1)})
                for day in sorted(days):
                    calculate(day)
                for day in sorted(pending_exports.copy()):
                    try:
                        export_day(day)
                    except Exception:
                        continue
                    pending_exports.remove(day)
        except Exception:
            log.exception("Плановый цикл не завершён; следующая попытка через %s сек", INTERVAL)
        stop.wait(INTERVAL)


@asynccontextmanager
async def lifespan(app):
    worker = threading.Thread(target=schedule, daemon=True)
    worker.start()
    yield
    stop.set()
    worker.join(timeout=5)


app = FastAPI(title="Aggregation Service", lifespan=lifespan, docs_url=None, redoc_url=None)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:58000", "http://127.0.0.1:58000"],
    allow_methods=["POST"],
    allow_headers=["Content-Type"],
)


def check_day(day):
    if day > today():
        raise HTTPException(422, "Нельзя пересчитать или экспортировать будущую дату")


@app.post("/recalculate")
def recalculate(date: date):
    check_day(date)
    try:
        with cycle_lock:
            return calculate(date)
    except Exception as exc:
        log.exception("Ошибка пересчёта date=%s", date)
        raise HTTPException(503, "Ошибка чтения ClickHouse или записи PostgreSQL") from exc


@app.post("/export")
def export(date: date):
    check_day(date)
    try:
        with cycle_lock:
            return export_day(date)
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:
        raise HTTPException(503, "Ошибка PostgreSQL или S3; экспорт можно повторить") from exc


@app.get("/health", include_in_schema=False)
def health():
    try:
        ch("SELECT 1")
        with pg() as conn:
            conn.execute("SELECT 1")
        s3().head_bucket(Bucket=BUCKET)
    except Exception as exc:
        raise HTTPException(503, "ClickHouse, PostgreSQL или S3 недоступен") from exc
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
