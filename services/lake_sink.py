"""Idempotent canonical-event sink for a local Parquet lakehouse lab.

The Kafka consumer in ``services/sink.py`` proves an at-least-once DuckDB sink.  This
module adds the next production-shaped boundary: a durable event ledger, batch-level
replay, and date-partitioned Parquet output.  DuckDB is the local emulator for the
transaction/log layer; the same contract can later be implemented with Spark
Structured Streaming and Iceberg.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import duckdb


@dataclass(frozen=True)
class LakeWriteResult:
    batch_id: str
    received_rows: int
    new_rows: int
    duplicate_rows: int
    parquet_files: tuple[str, ...]


def _parse_time(value: Any) -> datetime:
    text = str(value).replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _row(event: Mapping[str, Any]) -> tuple[Any, ...]:
    source = event.get("source") or {}
    required = ("event_id", "schema_version", "event_time", "ingested_at", "sensor_id", "temperature", "humidity", "status")
    missing = [field for field in required if event.get(field) in (None, "")]
    if missing:
        raise ValueError(f"canonical event is missing fields: {missing}")
    event_time = _parse_time(event["event_time"])
    return (
        str(event["event_id"]),
        str(event["schema_version"]),
        event_time,
        _parse_time(event["ingested_at"]),
        str(event["sensor_id"]),
        float(event["temperature"]),
        float(event["humidity"]),
        str(event["status"]),
        str(source.get("topic", "unknown")),
        int(source.get("partition", 0)),
        int(source.get("offset", 0)),
    )


def _initialize(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS lake_events (
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
            batch_id VARCHAR NOT NULL,
            event_date DATE NOT NULL,
            stored_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS exported_batches (
            batch_id VARCHAR PRIMARY KEY,
            row_count BIGINT NOT NULL,
            output_path VARCHAR NOT NULL,
            exported_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def write_canonical_events(
    events: Iterable[Mapping[str, Any]],
    output_dir: str | os.PathLike[str],
    batch_id: str,
    ledger_path: str | os.PathLike[str] | None = None,
) -> LakeWriteResult:
    """Persist a batch once and export it as event-date partitioned Parquet.

    Replaying the same batch is safe: the ledger returns the existing output and a
    duplicate event never creates a second row.  The final directory is only moved
    into place after DuckDB successfully writes the Parquet files.
    """

    if not batch_id or not re.fullmatch(r"[A-Za-z0-9_.-]+", batch_id):
        raise ValueError("batch_id must contain only letters, numbers, '.', '_' or '-'")
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    ledger = Path(ledger_path) if ledger_path else output / "_lake_ledger.duckdb"
    ledger.parent.mkdir(parents=True, exist_ok=True)
    materialized = list(events)
    received_rows = len(materialized)
    deduped: list[tuple[Any, ...]] = []
    seen: set[str] = set()
    for event in materialized:
        row = _row(event)
        if row[0] in seen:
            continue
        seen.add(row[0])
        deduped.append(row)

    connection = duckdb.connect(str(ledger))
    _initialize(connection)
    try:
        exported = connection.execute(
            "SELECT row_count, output_path FROM exported_batches WHERE batch_id = ?",
            [batch_id],
        ).fetchone()
        final_path = output / f"batch_id={batch_id}"
        if exported:
            files = tuple(sorted(str(path.relative_to(output)) for path in final_path.rglob("*.parquet")))
            return LakeWriteResult(batch_id, received_rows, 0, received_rows, files)

        connection.execute("BEGIN")
        connection.execute(
            """
            CREATE OR REPLACE TEMP TABLE input_events (
                event_id VARCHAR,
                schema_version VARCHAR,
                event_time TIMESTAMPTZ,
                ingested_at TIMESTAMPTZ,
                sensor_id VARCHAR,
                temperature DOUBLE,
                humidity DOUBLE,
                status VARCHAR,
                source_topic VARCHAR,
                source_partition INTEGER,
                source_offset BIGINT
            )
            """
        )
        connection.executemany(
            "INSERT INTO input_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            deduped,
        )
        existing_rows = connection.execute(
            """
            SELECT COUNT(*)
            FROM input_events AS input
            JOIN lake_events AS stored ON stored.event_id = input.event_id
            """
        ).fetchone()[0]
        new_rows = len(deduped) - int(existing_rows)
        connection.execute(
            """
            INSERT INTO lake_events (
                event_id, schema_version, event_time, ingested_at, sensor_id,
                temperature, humidity, status, source_topic, source_partition,
                source_offset, batch_id, event_date
            )
            SELECT event_id, schema_version, event_time, ingested_at, sensor_id,
                   temperature, humidity, status, source_topic, source_partition,
                   source_offset, ?, CAST(event_time AS DATE)
            FROM input_events AS input
            WHERE NOT EXISTS (
                SELECT 1 FROM lake_events AS stored WHERE stored.event_id = input.event_id
            )
            """,
            [batch_id],
        )
        connection.commit()

        rows_in_batch = connection.execute(
            "SELECT COUNT(*) FROM lake_events WHERE batch_id = ?", [batch_id]
        ).fetchone()[0]
        if rows_in_batch:
            staging_path = output / f".staging-{batch_id}"
            staging_path.mkdir(parents=False, exist_ok=False)
            connection.execute(
                "CREATE OR REPLACE TEMP TABLE current_batch AS SELECT * EXCLUDE (batch_id, stored_at) FROM lake_events LIMIT 0"
            )
            connection.execute(
                "INSERT INTO current_batch SELECT * EXCLUDE (batch_id, stored_at) FROM lake_events WHERE batch_id = ?",
                [batch_id],
            )
            quoted_path = str(staging_path).replace("'", "''")
            connection.execute(
                f"COPY (SELECT * FROM current_batch) TO '{quoted_path}' (FORMAT PARQUET, PARTITION_BY (event_date))"
            )
            if final_path.exists():
                raise FileExistsError(f"output already exists for batch {batch_id}")
            staging_path.rename(final_path)
        connection.execute(
            "INSERT INTO exported_batches(batch_id, row_count, output_path) VALUES (?, ?, ?)",
            [batch_id, rows_in_batch, str(final_path)],
        )
        connection.commit()
        files = tuple(sorted(str(path.relative_to(output)) for path in final_path.rglob("*.parquet"))) if rows_in_batch else ()
        return LakeWriteResult(
            batch_id=batch_id,
            received_rows=received_rows,
            new_rows=int(new_rows),
            duplicate_rows=received_rows - int(new_rows),
            parquet_files=files,
        )
    except Exception:
        try:
            connection.rollback()
        except duckdb.TransactionException:
            pass
        raise
    finally:
        connection.close()
