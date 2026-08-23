import unittest

from services.contract import DataQualityError, normalize_sensor_event


class SensorContractTest(unittest.TestCase):
    def test_normalizes_json_event_and_generates_stable_id(self):
        raw = {
            "timestamp": "2026-08-23T12:00:00Z",
            "sensor_id": "AI-FACTORY-001",
            "temperature": 87.5,
            "humidity": 42.4,
            "status": "RUNNING",
        }

        first = normalize_sensor_event(raw)
        second = normalize_sensor_event(raw)

        self.assertEqual(first.event_id, second.event_id)
        self.assertEqual(first.event_time, "2026-08-23T12:00:00Z")
        self.assertEqual(first.status, "RUNNING")

    def test_normalizes_text_event(self):
        raw = {"log": "[2026-08-23 12:00:00] ID=AI-FACTORY-001 | TEMP:87.5 | HUMI:42.4 | STAT:RUNNING"}

        event = normalize_sensor_event(raw)

        self.assertEqual(event.sensor_id, "AI-FACTORY-001")
        self.assertEqual(event.temperature, 87.5)
        self.assertEqual(event.humidity, 42.4)

    def test_rejects_out_of_range_temperature(self):
        raw = {
            "timestamp": "2026-08-23T12:00:00Z",
            "sensor_id": "AI-FACTORY-001",
            "temperature": 999,
            "humidity": 42.4,
            "status": "RUNNING",
        }

        with self.assertRaises(DataQualityError):
            normalize_sensor_event(raw)

    def test_rejects_non_finite_measurement(self):
        raw = {
            "timestamp": "2026-08-23T12:00:00Z",
            "sensor_id": "AI-FACTORY-001",
            "temperature": float("nan"),
            "humidity": 42.4,
            "status": "RUNNING",
        }

        with self.assertRaises(DataQualityError):
            normalize_sensor_event(raw)


if __name__ == "__main__":
    unittest.main()
