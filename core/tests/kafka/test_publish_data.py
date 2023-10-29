import unittest
from unittest.mock import patch, MagicMock
from kafka.publish_data import PublishData


class TestPublishData(unittest.TestCase):

    def setUp(self):
        self.publish_data = PublishData('localhost:9092', 'test-topic')

    @patch('kafka.publish_data.PublishData._PublishData__create_kafka_sink')
    def test_publish_data_to_kafka_topic(self, mock__create_kafka_sink):
        mock_kafka_sink = MagicMock()
        mock_kafka_sink.sink_to.return_value = None
        mock__create_kafka_sink.return_value = mock_kafka_sink

        mock_datastream = MagicMock()

        self.publish_data.publish_data_to_kafka_topic(mock_datastream)

        mock_datastream.map.assert_called_once_with(unittest.mock.ANY, output_type=self.publish_data.value_type_info)
        mock_kafka_sink.sink_to(mock_datastream.map.return_value)

        mock_kafka_sink.sink_to.assert_called_once()

