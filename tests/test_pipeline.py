import json
import random
import time
import unittest
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import requests
from confluent_kafka.admin import AdminClient, ConfigResource, ResourceType

from app.common import BROKERS, BUCKET, REGISTRY, SUBJECT, TOPIC, ch, ch_rows, pg, s3

MOVIE = "http://movie:8000"
AGGREGATION = "http://aggregation:8000"


def post(url, **kwargs):
    response = requests.post(url, timeout=120, **kwargs)
    response.raise_for_status()
    return response.json()


class PipelineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.prefix = "test-" + uuid4().hex
        # Pick unused historical dates, so exact expected metrics don't depend on previous runs.
        for _ in range(100):
            candidate = date(2000, 1, 1) + timedelta(days=random.randrange(7000))
            n = ch_rows(f"SELECT count() AS n FROM raw_events FINAL WHERE toDate(timestamp) "
                        f"BETWEEN toDate('{candidate}') AND addDays(toDate('{candidate}'), 7)")[0]["n"]
            if int(n) == 0:
                cls.day = candidate
                break
        else:
            raise RuntimeError("Не найдена свободная дата для тестовых событий")
        cls.events = []
        cls.days = [cls.day + timedelta(days=offset) for offset in (0, 1, 2, 7)]
        cls.addClassCleanup(cls.cleanup)

        def event(user, movie, kind, seconds, offset=0, session="main", minute=0):
            item = {"event_id": str(uuid4()), "user_id": cls.prefix + user,
                    "movie_id": movie, "event_type": kind,
                    "timestamp": datetime.combine(cls.day + timedelta(days=offset), datetime.min.time(),
                                                  tzinfo=timezone.utc).replace(hour=12, minute=minute, microsecond=123000).isoformat(),
                    "device_type": "TV", "session_id": cls.prefix + user + session,
                    "progress_seconds": seconds}
            cls.events.append(item)
            return item

        event("a", "film-a", "VIEW_STARTED", 0)
        event("a", "film-a", "VIEW_PAUSED", 200, minute=1)
        event("a", "film-a", "VIEW_RESUMED", 300, minute=2)
        event("a", "film-a", "VIEW_FINISHED", 600, minute=3)
        event("a", "film-a", "LIKED", 600, minute=4)
        event("b", "film-b", "VIEW_STARTED", 0)
        event("b", "film-b", "VIEW_FINISHED", 1200, minute=20)
        event("c", "film-c", "SEARCHED", 0)
        for user, movie, offset in (("a", "film-a", 1), ("a", "film-a", 7),
                                    ("b", "film-b", 2), ("c", "film-c", 1)):
            event(user, movie, "VIEW_STARTED", 0, offset, session=f"return-{offset}")
            event(user, movie, "VIEW_FINISHED", 60, offset, session=f"return-{offset}", minute=1)
        for item in cls.events:
            result = post(MOVIE + "/events", json=item)
            if result["event_id"] != item["event_id"]:
                raise AssertionError(result)
        # Same immutable event twice must not inflate aggregates after Kafka retries/re-delivery.
        post(MOVIE + "/events", json=cls.events[0])
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            cls.stored = ch_rows(f"SELECT * FROM raw_events FINAL WHERE startsWith(user_id, '{cls.prefix}')")
            if len(cls.stored) == len(cls.events):
                break
            time.sleep(1)
        else:
            raise AssertionError(f"В ClickHouse {len(cls.stored)} из {len(cls.events)} событий")

    @classmethod
    def cleanup(cls):
        ch(f"ALTER TABLE raw_events DELETE WHERE startsWith(user_id, '{cls.prefix}') SETTINGS mutations_sync=1")
        for day in cls.days:
            ch(f"ALTER TABLE daily_metrics DELETE WHERE metric_date = toDate('{day}') SETTINGS mutations_sync=1")
            with pg() as conn:
                conn.execute("DELETE FROM daily_metrics WHERE metric_date = %s", (day,))
            s3().delete_object(Bucket=BUCKET, Key=f"daily/{day}/aggregates.json")

    def metrics(self):
        with pg() as conn:
            return conn.execute("SELECT metric_name, dimension, value FROM daily_metrics "
                                "WHERE metric_date = %s ORDER BY metric_name, dimension", (self.day,)).fetchall()

    def test_01_schema_and_topic(self):
        response = requests.get(f"{REGISTRY}/subjects/{SUBJECT}/versions/latest", timeout=10)
        response.raise_for_status()
        registered = response.json()
        self.assertGreaterEqual(registered["version"], 1)
        self.assertEqual(json.loads(registered["schema"])["name"], "MovieEvent")
        admin = AdminClient({"bootstrap.servers": BROKERS})
        partitions = admin.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
        self.assertGreaterEqual(len(partitions), 3)
        self.assertTrue(all(len(partition.replicas) >= 2 for partition in partitions.values()))
        resource = ConfigResource(ResourceType.TOPIC, TOPIC)
        config = admin.describe_configs([resource])[resource].result(timeout=10)
        self.assertGreaterEqual(int(config["min.insync.replicas"].value), 1)

    def test_02_validation(self):
        changes = [{"event_id": "bad-uuid"}, {"event_type": "UNKNOWN"}, {"device_type": "WATCH"},
                   {"progress_seconds": -1}, {"progress_seconds": True},
                   {"timestamp": "2026-01-01T10:00:00"}, {"extra": "not-in-schema"}]
        for change in changes:
            with self.subTest(change=change):
                response = requests.post(MOVIE + "/events", json={**self.events[0], **change}, timeout=20)
                self.assertEqual(response.status_code, 422, response.text)

    def test_03_ingestion_fields(self):
        by_id = {row["event_id"]: row for row in self.stored}
        self.assertEqual(len(by_id), len(self.events))
        for item in self.events:
            row = by_id[item["event_id"]]
            for key in ("event_id", "user_id", "movie_id", "event_type", "device_type", "session_id", "progress_seconds"):
                self.assertEqual(row[key], item[key], (key, item, row))
            self.assertEqual(datetime.fromisoformat(row["timestamp"]).replace(tzinfo=timezone.utc),
                             datetime.fromisoformat(item["timestamp"]))

    def test_04_aggregates_and_idempotency(self):
        post(AGGREGATION + "/recalculate", params={"date": self.day.isoformat()})
        first = self.metrics()
        metrics = {(name, dimension): value for name, dimension, value in first}
        self.assertEqual(metrics[("dau", "")], 3)
        self.assertEqual(metrics[("avg_watch_seconds", "")], 900)
        self.assertEqual(metrics[("conversion", "")], 1)
        self.assertEqual(metrics[("movie_views", "film-a")], 1)
        self.assertEqual(metrics[("movie_views", "film-b")], 1)
        self.assertNotIn(("movie_views", "film-c"), metrics)
        self.assertEqual(metrics[("retention_d0", "")], 1)
        self.assertEqual(metrics[("retention_d1", "")], 0.5)
        self.assertEqual(metrics[("retention_d2", "")], 0.5)
        self.assertEqual(metrics[("retention_d7", "")], 0.5)
        self.assertEqual(metrics[("retention_d3", "")], 0)
        post(AGGREGATION + "/recalculate", params={"date": self.day.isoformat()})
        self.assertEqual(first, self.metrics())
        materialized = ch_rows(f"SELECT metric_name, dimension, value FROM daily_metrics FINAL "
                               f"WHERE metric_date = toDate('{self.day}') ORDER BY metric_name, dimension")
        self.assertEqual(first, [(row["metric_name"], row["dimension"], row["value"]) for row in materialized])

    def test_05_export_overwrite(self):
        post(AGGREGATION + "/recalculate", params={"date": self.day.isoformat()})
        post(AGGREGATION + "/export", params={"date": self.day.isoformat()})
        key = f"daily/{self.day}/aggregates.json"
        first = s3().get_object(Bucket=BUCKET, Key=key)["Body"].read()
        post(AGGREGATION + "/recalculate", params={"date": self.day.isoformat()})
        post(AGGREGATION + "/export", params={"date": self.day.isoformat()})
        second = s3().get_object(Bucket=BUCKET, Key=key)["Body"].read()
        exported = json.loads(second)
        self.assertNotEqual(first, second)  # computed_at must reflect the new calculation.
        self.assertEqual(len(exported), len(self.metrics()))
        self.assertTrue(all(row["date"] == str(self.day) and row["computed_at"].endswith("+00:00") for row in exported))
        self.assertEqual({(r["metric_name"], r["dimension"]): r["value"] for r in exported},
                         {(name, dim): value for name, dim, value in self.metrics()})
        objects = s3().list_objects_v2(Bucket=BUCKET, Prefix=f"daily/{self.day}/")["Contents"]
        self.assertEqual([row["Key"] for row in objects], [key])

    def test_06_dashboard_queries(self):
        response = requests.get("http://grafana:3000/api/dashboards/uid/online-cinema",
                                auth=("admin", "cinema"), timeout=10)
        response.raise_for_status()
        panels = response.json()["dashboard"]["panels"]
        self.assertGreaterEqual(len(panels), 3)
        for panel in panels:
            for target in panel["targets"]:
                self.assertIn("daily_metrics FINAL", target["rawSql"])
                self.assertNotIn("raw_events", target["rawSql"])
                response = requests.post("http://grafana:3000/api/ds/query", auth=("admin", "cinema"),
                    json={"from": "0", "to": str(int(time.time() * 1000)), "queries": [target]}, timeout=30)
                response.raise_for_status()
                result = response.json()["results"][target["refId"]]
                self.assertNotIn("error", result, result)


if __name__ == "__main__":
    unittest.main()
