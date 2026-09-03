from __future__ import annotations

import unittest

from services.metrics import MetricsRegistry


class MetricsRegistryTest(unittest.TestCase):
    def test_render_includes_help_type_labels_and_values(self) -> None:
        registry = MetricsRegistry()
        registry.inc(
            "sensor_records_total",
            labels={"result": "clean", "topic": "factory.sensor.clean.v1"},
            help_text="Number of records by result.",
        )
        registry.inc(
            "sensor_records_total",
            labels={"result": "clean", "topic": "factory.sensor.clean.v1"},
            value=2,
            help_text="Number of records by result.",
        )
        registry.set_gauge(
            "sensor_consumer_lag",
            4,
            labels={"partition": 0, "topic": "factory.sensor.clean.v1"},
            help_text="Current consumer lag.",
        )

        rendered = registry.render()

        self.assertIn("# TYPE sensor_records_total counter", rendered)
        self.assertIn(
            'sensor_records_total{result="clean",topic="factory.sensor.clean.v1"} 3',
            rendered,
        )
        self.assertIn("# TYPE sensor_consumer_lag gauge", rendered)
        self.assertIn(
            'sensor_consumer_lag{partition="0",topic="factory.sensor.clean.v1"} 4',
            rendered,
        )

    def test_label_values_are_escaped(self) -> None:
        registry = MetricsRegistry()
        registry.inc(
            "records_total",
            labels={"source": 'line"one\\two\nthree'},
            help_text="Records.",
        )

        self.assertIn('source="line\\"one\\\\two\\nthree"', registry.render())

    def test_counter_and_gauge_reject_non_finite_values(self) -> None:
        registry = MetricsRegistry()
        with self.assertRaises(ValueError):
            registry.inc("records_total", value=float("nan"))
        with self.assertRaises(ValueError):
            registry.set_gauge("lag", float("inf"))


if __name__ == "__main__":
    unittest.main()
