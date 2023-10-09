import unittest
from unittest.mock import patch, Mock
from pyflink.datastream import StreamExecutionEnvironment
from kafka.consume_data import ConsumeData

class TestSourceData(unittest.TestCase):

    def setUp(self) -> None:
        self.env = StreamExecutionEnvironment.get_execution_environment()
        jarfile = "file:///core/lib/flink-sql-connector-kafka-1.17.1.jar"
        self.env.add_jars(jarfile)
        self.consume_data = ConsumeData(self.env)

    @patch('kafka.consume_data.ConsumeData._ConsumeData__create_kafka_source')
    @patch('kafka.consume_data.ConsumeData._ConsumeData__create_data_stream')
    def test_consume_data(self, mock_create_data_stream, mock_create_kafka_source):
        mock_kafka_source = Mock()
        mock_data_stream = Mock()

        mock_create_kafka_source.return_value = mock_kafka_source
        mock_data_stream.collect.return_value = [1, 2, 3]
        mock_create_data_stream.return_value = mock_data_stream

        ds = self.consume_data.get_kafka_data()

        mock_create_kafka_source.assert_called_once()
        mock_create_data_stream.assert_called_once_with(mock_kafka_source)

        elements = ds.collect()

        self.assertEqual(elements, [1, 2, 3])
