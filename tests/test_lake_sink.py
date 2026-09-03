from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

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


class LakeSinkTest(unittest.TestCase):
    def test_sink_writes_date_partition_and_deduplicates_replay(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_dir = Path(temporary_directory)
            first = write_canonical_events(
                [
                    _event("event-1"),
                    _event("event-1"),
                    _event("event-2", "2026-09-04T00:00:00Z"),
                ],
                output_dir,
                "batch-1",
            )
            second = write_canonical_events(
                [_event("event-1"), _event("event-2", "2026-09-04T00:00:00Z")],
                output_dir,
                "batch-1",
            )

            self.assertEqual(first.new_rows, 2)
            self.assertEqual(first.duplicate_rows, 1)
            self.assertTrue(
                any("event_date=2026-09-03" in path for path in first.parquet_files)
            )
            self.assertTrue(
                any("event_date=2026-09-04" in path for path in first.parquet_files)
            )
            self.assertEqual(second.new_rows, 0)
            self.assertEqual(second.parquet_files, first.parquet_files)

            connection = duckdb.connect(str(output_dir / "_lake_ledger.duckdb"), read_only=True)
            try:
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM lake_events").fetchone()[0],
                    2,
                )
            finally:
                connection.close()

    def test_sink_rejects_unsafe_batch_id(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            with self.assertRaisesRegex(ValueError, "batch_id"):
                write_canonical_events([_event("event-1")], temporary_directory, "../escape")

    def test_sink_requires_kafka_source_lineage(self) -> None:
        event = _event("event-1")
        del event["source"]

        with tempfile.TemporaryDirectory() as temporary_directory:
            with self.assertRaisesRegex(ValueError, "source"):
                write_canonical_events([event], temporary_directory, "batch-lineage")

    def test_sink_recovers_when_marker_was_not_recorded(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_dir = Path(temporary_directory)
            first = write_canonical_events([_event("event-1")], output_dir, "batch-recovery")

            connection = duckdb.connect(str(output_dir / "_lake_ledger.duckdb"))
            try:
                connection.execute("DELETE FROM exported_batches WHERE batch_id = 'batch-recovery'")
                connection.commit()
            finally:
                connection.close()

            recovered = write_canonical_events(
                [_event("event-1")], output_dir, "batch-recovery"
            )

            self.assertEqual(recovered.new_rows, 0)
            self.assertEqual(recovered.parquet_files, first.parquet_files)
            connection = duckdb.connect(str(output_dir / "_lake_ledger.duckdb"), read_only=True)
            try:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM exported_batches WHERE batch_id = 'batch-recovery'"
                    ).fetchone()[0],
                    1,
                )
            finally:
                connection.close()

    def test_sink_records_empty_batch_without_executemany_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            result = write_canonical_events([], temporary_directory, "batch-empty")

            self.assertEqual(result.received_rows, 0)
            self.assertEqual(result.new_rows, 0)
            self.assertEqual(result.duplicate_rows, 0)
            self.assertEqual(result.parquet_files, ())


if __name__ == "__main__":
    unittest.main()
