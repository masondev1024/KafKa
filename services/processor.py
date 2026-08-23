"""At-least-once Kafka normalizer with a data-quality dead-letter topic."""

from __future__ import annotations

import json
import logging
import os
import signal
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from .contract import DataQualityError, normalize_sensor_event


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
LOGGER = logging.getLogger("sensor-processor")
STOP_REQUESTED = False


def _stop_handler(_signum: int, _frame: Any) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _decode(raw: bytes | None) -> Any:
    if raw is None:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return raw.decode("utf-8", errors="replace")


def _build_dlq(raw: Any, error: Exception, topic: str, partition: int, offset: int) -> dict[str, Any]:
    return {
        "schema_version": "factory-sensor-dlq.v1",
        "failed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "error_type": type(error).__name__,
        "error_message": str(error),
        "source": {"topic": topic, "partition": partition, "offset": offset},
        "raw_event": raw,
    }


def run() -> None:
    signal.signal(signal.SIGTERM, _stop_handler)
    signal.signal(signal.SIGINT, _stop_handler)

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    raw_topics = tuple(
        topic.strip()
        for topic in os.getenv(
            "RAW_TOPICS", "factory.sensor.raw.json.v1,factory.sensor.raw.text.v1"
        ).split(",")
        if topic.strip()
    )
    clean_topic = os.getenv("CLEAN_TOPIC", "factory.sensor.clean.v1")
    dlq_topic = os.getenv("DLQ_TOPIC", "factory.sensor.dlq.v1")
    group_id = os.getenv("CONSUMER_GROUP", "sensor-normalizer-v1")

    consumer = KafkaConsumer(
        *raw_topics,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=50,
        request_timeout_ms=30_000,
        session_timeout_ms=10_000,
    )
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        retries=10,
        linger_ms=10,
        compression_type="gzip",
        key_serializer=lambda key: key.encode("utf-8") if key else None,
        value_serializer=_json_bytes,
    )

    LOGGER.info(
        "processor_started raw_topics=%s clean_topic=%s dlq_topic=%s group_id=%s",
        raw_topics,
        clean_topic,
        dlq_topic,
        group_id,
    )
    try:
        while not STOP_REQUESTED:
            records = consumer.poll(timeout_ms=1_000, max_records=50)
            for _tp, messages in records.items():
                for message in messages:
                    raw = _decode(message.value)
                    try:
                        normalized = normalize_sensor_event(raw)
                        output = normalized.as_dict(
                            source_topic=message.topic,
                            source_partition=message.partition,
                            source_offset=message.offset,
                        )
                        producer.send(clean_topic, key=normalized.sensor_id, value=output).get(timeout=10)
                        LOGGER.info(
                            "event_normalized event_id=%s sensor_id=%s source=%s:%s:%s",
                            normalized.event_id,
                            normalized.sensor_id,
                            message.topic,
                            message.partition,
                            message.offset,
                        )
                    except (DataQualityError, TypeError, ValueError, KeyError) as error:
                        dlq_record = _build_dlq(
                            raw, error, message.topic, message.partition, message.offset
                        )
                        producer.send(dlq_topic, value=dlq_record).get(timeout=10)
                        LOGGER.warning(
                            "event_quarantined topic=%s partition=%s offset=%s error=%s",
                            message.topic,
                            message.partition,
                            message.offset,
                            error,
                        )
                    except KafkaError:
                        LOGGER.exception("kafka_publish_failed; offset will be retried")
                        raise
                    consumer.commit()
    finally:
        producer.flush(timeout=10)
        producer.close()
        consumer.close()
        LOGGER.info("processor_stopped")


if __name__ == "__main__":
    run()
