from __future__ import annotations

import duckdb

from services.lake_sink import write_canonical_events


def _event(event_id: str, event_time: str = "2026-09-03T00:00:00Z") -> dict[str, object]:
    return {
        "event_id": event_id,
        "schema_version": "factory-sensor.v1",
        "event_time": event_time,
        "ingested_at": "2026-09-03T00:00:01Z",
        "sensor_id": "AI-FACTORY-001",
        "temperature": 87.5,
        "humidity": 42.4,
        "status": "RUNNING",
        "source": {"topic": "factory.sensor.clean.v1", "partition": 0, "offset": 10},
    }


def test_sink_writes_date_partition_and_deduplicates_replay(tmp_path) -> None:
    first = write_canonical_events(
        [_event("event-1"), _event("event-1"), _event("event-2", "2026-09-04T00:00:00Z")],
        tmp_path,
        "batch-1",
    )
    second = write_canonical_events(
        [_event("event-1"), _event("event-2", "2026-09-04T00:00:00Z")],
        tmp_path,
        "batch-1",
    )

    assert first.new_rows == 2
    assert first.duplicate_rows == 1
    assert any("event_date=2026-09-03" in path for path in first.parquet_files)
    assert any("event_date=2026-09-04" in path for path in first.parquet_files)
    assert second.new_rows == 0
    assert second.parquet_files == first.parquet_files

    connection = duckdb.connect(str(tmp_path / "_lake_ledger.duckdb"), read_only=True)
    try:
        assert connection.execute("SELECT COUNT(*) FROM lake_events").fetchone()[0] == 2
    finally:
        connection.close()


def test_sink_rejects_unsafe_batch_id(tmp_path) -> None:
    try:
        write_canonical_events([_event("event-1")], tmp_path, "../escape")
    except ValueError as exc:
        assert "batch_id" in str(exc)
    else:
        raise AssertionError("unsafe batch id was accepted")
