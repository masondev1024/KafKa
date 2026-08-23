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
from typing import Any

import duckdb
from kafka import KafkaConsumer


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


def run() -> None:
    signal.signal(signal.SIGTERM, _stop_handler)
    signal.signal(signal.SIGINT, _stop_handler)

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("CLEAN_TOPIC", "factory.sensor.clean.v1")
    group_id = os.getenv("SINK_CONSUMER_GROUP", "sensor-duckdb-sink-v1")
    database_path = os.getenv("DUCKDB_PATH", "/data/sensor.duckdb")

    os.makedirs(os.path.dirname(database_path) or ".", exist_ok=True)
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
    try:
        while not STOP_REQUESTED:
            records = consumer.poll(timeout_ms=1_000, max_records=50)
            for _tp, messages in records.items():
                for message in messages:
                    event = message.value
                    source = event["source"]
                    connection.execute(
                        """
                        INSERT INTO sensor_events (
                            event_id, schema_version, event_time, ingested_at,
                            sensor_id, temperature, humidity, status,
                            source_topic, source_partition, source_offset
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (event_id) DO NOTHING
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
                    )
                    connection.commit()
                    consumer.commit()
                    LOGGER.info(
                        "event_stored event_id=%s sensor_id=%s kafka_offset=%s",
                        event["event_id"],
                        event["sensor_id"],
                        message.offset,
                    )
    finally:
        consumer.close()
        connection.close()
        LOGGER.info("sink_stopped")


if __name__ == "__main__":
    run()
