import unittest
from unittest.mock import patch, Mock
from kafka.data_processor import DataProcessor

class DataProcessorTest(unittest.TestCase):

    def setUp(self) -> None:
        self.extracting_record_attributes = Mock()
        self.group_and_filter = Mock()
        self.data_processor = DataProcessor(self.extracting_record_attributes, self.group_and_filter)

    @patch('kafka.data_processor.DataProcessor._DataProcessor__process_data_stream')
    def test_get_data_stream(self, mock_process_data_stream):
        result_stream = Mock()
        self.extracting_record_attributes.return_value = [('abc', 'gold1', '1613131')]
        mock_process_data_stream.return_value = ('abc', 'gold1', '1613131')
        actual = self.data_processor.get_data_stream(result_stream)
        self.assertEqual(actual, ('abc', 'gold1', '1613131'))
