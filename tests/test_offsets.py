from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from kafka import TopicPartition

from services.processor import _commit_message as commit_processor_message
from services.sink import _commit_message as commit_sink_message


class ExplicitOffsetCommitTest(unittest.TestCase):
    def test_processor_commits_only_processed_record(self) -> None:
        consumer = Mock()
        message = SimpleNamespace(topic="factory.sensor.raw.json.v1", partition=2, offset=7)

        commit_processor_message(consumer, message)

        offsets = consumer.commit.call_args.kwargs["offsets"]
        self.assertEqual(offsets[TopicPartition(message.topic, message.partition)].offset, 8)

    def test_sink_commits_only_persisted_record(self) -> None:
        consumer = Mock()
        message = SimpleNamespace(topic="factory.sensor.clean.v1", partition=1, offset=11)

        commit_sink_message(consumer, message)

        offsets = consumer.commit.call_args.kwargs["offsets"]
        self.assertEqual(offsets[TopicPartition(message.topic, message.partition)].offset, 12)


if __name__ == "__main__":
    unittest.main()
