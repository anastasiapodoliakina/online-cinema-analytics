import json
import logging
import os
import time
from pathlib import Path

import boto3
import psycopg
import requests
from botocore.config import Config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("cinema")
ROOT = Path(__file__).resolve().parent.parent
BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
REGISTRY = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC = "movie-events"
SUBJECT = TOPIC + "-value"
CH_URL = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123")
PG_DSN = os.getenv("POSTGRES_DSN", "postgresql://cinema:cinema@postgres:5432/cinema")
BUCKET = "movie-analytics"
SCHEMA_TEXT = (ROOT / "schemas/movie-event-v1.avsc").read_text()
SCHEMA = json.loads(SCHEMA_TEXT)


def retry(operation, attempts=5):
    for attempt in range(attempts):
        try:
            return operation()
        except Exception:
            log.exception("Операция %s не выполнена, попытка %s/%s", operation.__name__, attempt + 1, attempts)
            if attempt + 1 == attempts:
                raise
            time.sleep(min(0.5 * 2**attempt, 8))


def ch(sql, rows=None):
    params = {"database": "cinema", "user": "cinema", "password": "cinema"}
    if rows is None:
        response = requests.post(CH_URL, params=params, data=sql.encode(), timeout=90)
    else:
        params["query"] = sql + " FORMAT JSONEachRow"
        data = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)
        response = requests.post(CH_URL, params=params, data=data.encode(), timeout=90)
    if not response.ok:
        raise RuntimeError(f"ClickHouse HTTP {response.status_code}: {response.text[:2000]}")
    return response.text


def ch_rows(sql):
    return json.loads(ch(sql + " FORMAT JSON"))["data"]


def pg():
    return psycopg.connect(PG_DSN, connect_timeout=5)


def s3():
    return boto3.client(
        "s3", endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY", "cinema"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY", "cinema-secret"),
        region_name="us-east-1",
        config=Config(connect_timeout=5, read_timeout=15, retries={"max_attempts": 2},
                      s3={"addressing_style": "path"}),
    )
