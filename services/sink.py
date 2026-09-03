"""Idempotent local sink for the normalized Kafka topic.

DuckDB is used only as a local analytical sink for the PoC. The unique
event_id constraint demonstrates the same protection a production lakehouse
sink needs when an at-least-once consumer retries after a crash.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import time
from typing import Any

import duckdb
from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata

from .metrics import MetricsRegistry, metrics_port_from_env, start_metrics_server

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
LOGGER = logging.getLogger("sensor-sink")
STOP_REQUESTED = False


def _stop_handler(_signum: int, _frame: Any) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True


def _initialize_schema(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS sensor_events (
            event_id VARCHAR PRIMARY KEY,
            schema_version VARCHAR NOT NULL,
            event_time TIMESTAMPTZ NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL,
            sensor_id VARCHAR NOT NULL,
            temperature DOUBLE NOT NULL,
            humidity DOUBLE NOT NULL,
            status VARCHAR NOT NULL,
            source_topic VARCHAR NOT NULL,
            source_partition INTEGER NOT NULL,
            source_offset BIGINT NOT NULL,
            stored_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def _commit_message(consumer: KafkaConsumer, message: Any) -> None:
    """Commit only the Kafka record persisted by the sink."""

    topic_partition = TopicPartition(message.topic, message.partition)
    consumer.commit(
        offsets={topic_partition: OffsetAndMetadata(message.offset + 1, None)}
    )


def run() -> None:
    signal.signal(signal.SIGTERM, _stop_handler)
    signal.signal(signal.SIGINT, _stop_handler)

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("CLEAN_TOPIC", "factory.sensor.clean.v1")
    group_id = os.getenv("SINK_CONSUMER_GROUP", "sensor-duckdb-sink-v1")
    database_path = os.getenv("DUCKDB_PATH", "/data/sensor.duckdb")

    metrics = MetricsRegistry()
    metrics_server = start_metrics_server(metrics, metrics_port_from_env(9101))
    os.makedirs(os.path.dirname(database_path) or ".", exist_ok=True)
    connection: duckdb.DuckDBPyConnection | None = None
    consumer: KafkaConsumer | None = None

    try:
        connection = duckdb.connect(database_path)
        _initialize_schema(connection)
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=50,
            value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
        )

        LOGGER.info("sink_started topic=%s group_id=%s database=%s", topic, group_id, database_path)
        while not STOP_REQUESTED:
            records = consumer.poll(timeout_ms=1_000, max_records=50)
            for _tp, messages in records.items():
                for message in messages:
                    event = message.value
                    source = event["source"]
                    try:
                        inserted = connection.execute(
                            """
                            INSERT INTO sensor_events (
                                event_id, schema_version, event_time, ingested_at,
                                sensor_id, temperature, humidity, status,
                                source_topic, source_partition, source_offset
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT (event_id) DO NOTHING
                            RETURNING event_id
                            """,
                            [
                                event["event_id"],
                                event["schema_version"],
                                event["event_time"],
                                event["ingested_at"],
                                event["sensor_id"],
                                event["temperature"],
                                event["humidity"],
                                event["status"],
                                source["topic"],
                                source["partition"],
                                source["offset"],
                            ],
                        ).fetchone()
                        connection.commit()
                    except Exception:
                        connection.rollback()
                        metrics.inc(
                            "sensor_sink_failures_total",
                            help_text="Number of records that failed before durable sink commit.",
                        )
                        LOGGER.exception(
                            "event_store_failed topic=%s partition=%s offset=%s",
                            message.topic,
                            message.partition,
                            message.offset,
                        )
                        raise

                    result = "stored" if inserted else "duplicate"
                    metrics.inc(
                        "sensor_sink_records_total",
                        labels={"result": result},
                        help_text="Number of canonical events handled by sink outcome.",
                    )
                    _commit_message(consumer, message)
                    metrics.set_gauge(
                        "sensor_sink_last_success_unixtime",
                        time.time(),
                        help_text="Unix timestamp of the last successfully persisted record.",
                    )
                    LOGGER.info(
                        "event_%s event_id=%s sensor_id=%s kafka_offset=%s",
                        result,
                        event["event_id"],
                        event["sensor_id"],
                        message.offset,
                    )
    finally:
        if consumer is not None:
            consumer.close()
        if connection is not None:
            connection.close()
        if metrics_server is not None:
            metrics_server.shutdown()
            metrics_server.server_close()
        LOGGER.info("sink_stopped")


if __name__ == "__main__":
    run()
