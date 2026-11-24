import requests
from confluent_kafka.admin import AdminClient, NewTopic
from botocore.exceptions import ClientError

from app.common import BROKERS, BUCKET, REGISTRY, ROOT, SCHEMA_TEXT, SUBJECT, TOPIC, ch, log, pg, retry, s3


def initialize():
    admin = AdminClient({"bootstrap.servers": BROKERS})
    existing = admin.list_topics(timeout=15).topics
    if TOPIC not in existing:
        result = admin.create_topics([NewTopic(TOPIC, num_partitions=3, replication_factor=2,
                                              config={"min.insync.replicas": "1"})])
        result[TOPIC].result(timeout=30)
    response = requests.put(f"{REGISTRY}/config/{SUBJECT}", json={"compatibility": "BACKWARD"}, timeout=15)
    response.raise_for_status()
    response = requests.post(f"{REGISTRY}/subjects/{SUBJECT}/versions",
                             json={"schemaType": "AVRO", "schema": SCHEMA_TEXT}, timeout=15)
    response.raise_for_status()
    version = requests.post(f"{REGISTRY}/subjects/{SUBJECT}", json={"schema": SCHEMA_TEXT}, timeout=15)
    version.raise_for_status()
    log.info("Схема movie-event-v1: subject=%s version=%s id=%s", SUBJECT,
             version.json()["version"], response.json()["id"])
    for statement in (ROOT / "sql/clickhouse.sql").read_text().split(";"):
        if statement.strip():
            ch(statement)
    with pg() as conn:
        conn.execute((ROOT / "sql/postgres.sql").read_text())
    client = s3()
    try:
        client.head_bucket(Bucket=BUCKET)
    except ClientError as exc:
        if exc.response["Error"]["Code"] not in ("404", "NoSuchBucket", "NotFound"):
            raise
        client.create_bucket(Bucket=BUCKET)
    log.info("Топик, схема, таблицы и бакет готовы")


if __name__ == "__main__":
    retry(initialize, attempts=10)
