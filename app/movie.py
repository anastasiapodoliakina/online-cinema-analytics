import argparse
import io
import json
import random
import struct
import threading
import time
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime, timezone
from enum import Enum
from typing import Annotated
from uuid import UUID, uuid4

import requests
import uvicorn
from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException, Request
from fastapi.openapi.docs import get_swagger_ui_html
from fastavro import parse_schema, schemaless_writer
from fastavro.validation import validate
from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.common import BROKERS, REGISTRY, SCHEMA_TEXT, SUBJECT, TOPIC, log, retry


class EventType(str, Enum):
    VIEW_STARTED = "VIEW_STARTED"
    VIEW_FINISHED = "VIEW_FINISHED"
    VIEW_PAUSED = "VIEW_PAUSED"
    VIEW_RESUMED = "VIEW_RESUMED"
    LIKED = "LIKED"
    SEARCHED = "SEARCHED"


class DeviceType(str, Enum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TV = "TV"
    TABLET = "TABLET"


class MovieEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_id: UUID
    user_id: Annotated[str, Field(min_length=1)]
    movie_id: Annotated[str, Field(min_length=1)]
    event_type: EventType
    timestamp: datetime
    device_type: DeviceType
    session_id: Annotated[str, Field(min_length=1)]
    progress_seconds: Annotated[int, Field(strict=True, ge=0, le=2147483647)]

    @field_validator("timestamp")
    @classmethod
    def utc_timestamp(cls, value):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp должен содержать часовой пояс UTC")
        value = value.astimezone(timezone.utc)
        if value < datetime(1970, 1, 1, tzinfo=timezone.utc):
            raise ValueError("timestamp должен быть не раньше 1970-01-01")
        return value.replace(microsecond=value.microsecond // 1000 * 1000)


class Publisher:
    def __init__(self):
        def load_schema():
            response = requests.post(f"{REGISTRY}/subjects/{SUBJECT}",
                                     json={"schema": SCHEMA_TEXT}, timeout=10)
            response.raise_for_status()
            return response.json()

        registered = retry(load_schema)
        self.schema_id = registered["id"]
        self.schema = parse_schema(json.loads(registered["schema"]))
        self.producer = Producer({
            "bootstrap.servers": BROKERS,
            "acks": "all",
            "enable.idempotence": True,
            "retries": 2147483647,
            "retry.backoff.ms": 200,
            "retry.backoff.max.ms": 5000,
            "delivery.timeout.ms": 30000,
            "request.timeout.ms": 10000,
        })

    def publish(self, event):
        record = event.model_dump(mode="json")
        record["timestamp"] = event.timestamp
        validate(record, self.schema, strict=True)
        buffer = io.BytesIO()
        # Confluent Avro: magic byte (0), big-endian schema ID, Avro record.
        buffer.write(struct.pack(">bI", 0, self.schema_id))
        schemaless_writer(buffer, self.schema, record)
        complete = threading.Event()
        errors = []

        def delivered(error, message):
            if error is not None:
                errors.append(str(error))
            complete.set()

        self.producer.produce(TOPIC, key=event.user_id.encode(), value=buffer.getvalue(), on_delivery=delivered)
        deadline = time.monotonic() + 35
        while not complete.is_set() and time.monotonic() < deadline:
            self.producer.poll(0.1)
        if not complete.is_set() or errors:
            raise RuntimeError(errors[0] if errors else "Истекло ожидание подтверждения Kafka")
        log.info("Опубликовано event_id=%s event_type=%s timestamp=%s",
                 event.event_id, event.event_type.value, event.timestamp.isoformat())


@asynccontextmanager
async def lifespan(app):
    app.state.publisher = Publisher()
    yield
    app.state.publisher.producer.flush(10)


app = FastAPI(title="Online Cinema", lifespan=lifespan, openapi_url=None,
              docs_url=None, redoc_url=None)


@app.get("/docs", include_in_schema=False)
def docs():
    return get_swagger_ui_html(openapi_url="/openapi.json", title="Online Cinema — Swagger")


@app.get("/openapi.json", include_in_schema=False)
def combined_openapi(request: Request):
    schema = deepcopy(app.openapi())
    try:
        response = requests.get("http://aggregation:8000/openapi.json", timeout=5)
        response.raise_for_status()
        aggregation_schema = response.json()
    except (requests.RequestException, ValueError) as exc:
        raise HTTPException(503, "Не удалось загрузить описание Aggregation Service") from exc

    for path in schema["paths"].values():
        for operation in path.values():
            operation["tags"] = ["События"]
    aggregation_url = str(request.url.replace(port=58002, path="", query=""))
    for path, methods in aggregation_schema["paths"].items():
        for operation in methods.values():
            operation["tags"] = ["Метрики и экспорт"]
            operation["servers"] = [{"url": aggregation_url}]
        schema["paths"][path] = methods
    schema.setdefault("components", {}).setdefault("schemas", {}).update(
        aggregation_schema.get("components", {}).get("schemas", {}))
    return schema


@app.get("/health", include_in_schema=False)
def health():
    try:
        app.state.publisher.producer.list_topics(TOPIC, timeout=5)
        requests.get(f"{REGISTRY}/subjects", timeout=5).raise_for_status()
    except Exception as exc:
        raise HTTPException(503, "Kafka или Schema Registry недоступен") from exc
    return {"status": "ok"}


@app.post("/events", status_code=201)
def publish_event(event: MovieEvent):
    try:
        app.state.publisher.publish(event)
    except Exception as exc:
        log.exception("Ошибка публикации event_id=%s", event.event_id)
        raise HTTPException(503, "Не удалось подтвердить публикацию события") from exc
    return {"event_id": str(event.event_id)}


def generate(sessions, interval):
    publisher = Publisher()
    rng = random.Random()
    completed = 0
    try:
        while sessions == 0 or completed < sessions:
            user = f"user-{rng.randint(1, 20)}"
            movie = f"movie-{rng.randint(1, 10)}"
            session = str(uuid4())
            device = rng.choice(list(DeviceType))
            progress = 0
            sequence = [EventType.SEARCHED, EventType.VIEW_STARTED, EventType.VIEW_PAUSED,
                        EventType.VIEW_RESUMED, EventType.VIEW_FINISHED, EventType.LIKED]
            for kind in sequence:
                if kind in (EventType.VIEW_PAUSED, EventType.VIEW_RESUMED, EventType.VIEW_FINISHED):
                    progress += rng.randint(1, 60)
                publisher.publish(MovieEvent(event_id=uuid4(), user_id=user, movie_id=movie,
                    session_id=session, device_type=device, event_type=kind,
                    timestamp=datetime.now(timezone.utc), progress_seconds=progress))
                time.sleep(interval)
            completed += 1
    finally:
        publisher.producer.flush(10)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--generate", action="store_true")
    parser.add_argument("--sessions", type=int, default=0, help="0 — непрерывный поток")
    parser.add_argument("--interval", type=float, default=1, help="Пауза между событиями, секунды")
    args = parser.parse_args()
    if args.sessions < 0 or args.interval < 0:
        parser.error("sessions и interval не могут быть отрицательными")
    if args.generate:
        generate(args.sessions, args.interval)
    else:
        uvicorn.run(app, host="0.0.0.0", port=8000)
