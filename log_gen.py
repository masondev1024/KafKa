"""Generate deterministic, replayable sensor events for the local PoC."""

from __future__ import annotations

import datetime as dt
import json
import os
import random
import time
from pathlib import Path


SENSOR_IDS = ("AI-FACTORY-001", "AI-FACTORY-002", "AI-FACTORY-003")
STATUSES = ("RUNNING", "IDLE", "STOPPED")


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc


def generate_event(rng: random.Random, invalid_rate: float) -> dict[str, object]:
    event = {
        "timestamp": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "sensor_id": rng.choice(SENSOR_IDS),
        "temperature": round(rng.uniform(70.0, 100.0), 1),
        "humidity": round(rng.uniform(30.0, 70.0), 1),
        "status": rng.choice(STATUSES),
    }

    # Invalid records are intentional: they exercise the DLQ and data-quality
    # monitoring path instead of hiding bad upstream data.
    if rng.random() < invalid_rate:
        event["temperature"] = 999.0
    return event


def write_event(event: dict[str, object], log_dir: Path, log_format: str = "both") -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    if log_format in {"both", "json"}:
        with (log_dir / "sensor_json.log").open("a", encoding="utf-8") as json_file:
            json_file.write(json.dumps(event, ensure_ascii=False) + "\n")

    if log_format in {"both", "text"}:
        text = (
            f"[{event['timestamp']}] ID={event['sensor_id']} | "
            f"TEMP:{event['temperature']} | HUMI:{event['humidity']} | "
            f"STAT:{event['status']}"
        )
        with (log_dir / "sensor_text.log").open("a", encoding="utf-8") as text_file:
            text_file.write(text + "\n")


def main() -> None:
    log_dir = Path(os.getenv("LOG_DIR", "./sensor_logs"))
    interval_seconds = _env_float("INTERVAL_SECONDS", 2.0)
    invalid_rate = _env_float("INVALID_RATE", 0.05)
    log_format = os.getenv("LOG_FORMAT", "both").lower()
    if log_format not in {"both", "json", "text"}:
        raise ValueError("LOG_FORMAT must be one of: both, json, text")
    if not 0.0 <= invalid_rate <= 1.0:
        raise ValueError("INVALID_RATE must be between 0 and 1")

    rng = random.Random(os.getenv("RANDOM_SEED"))
    print(
        f"로그 발생 시작: directory={log_dir} interval={interval_seconds}s "
        f"invalid_rate={invalid_rate} format={log_format}. 종료: Ctrl+C"
    )
    try:
        while True:
            event = generate_event(rng, invalid_rate)
            write_event(event, log_dir, log_format)
            print(f"로그 발생 완료 event_time={event['timestamp']} sensor_id={event['sensor_id']}")
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        print("로그 발생 종료")


if __name__ == "__main__":
    main()
