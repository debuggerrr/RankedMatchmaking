import unittest
import datetime
from unittest.mock import MagicMock, patch
from services.extract_record_attributes import ExtractRecordAttributes


class TestExtractingRecordAttributes(unittest.TestCase):

    @patch('services.extract_record_attributes.ast.literal_eval')
    def test_process(self, mock_literal_eval):
        mock_context = MagicMock()
        mock_element1 = ('("user1", "gold1")')
        mock_element2 = ('("user2", "gold2")')
        mock_element3 = ('("user3", "gold3")')
        mock_element4 = ('("user4", "silver1")')
        mock_element5 = ('("user5", "silver2")')
        mock_element6 = ('("user6", "silver3")')

        timestamp = str(int(datetime.datetime.now().timestamp()))
        mock_literal_eval.side_effect = lambda s: eval(s)

        extractor = ExtractRecordAttributes()

        elements = [mock_element1, mock_element2, mock_element3, mock_element4, mock_element5, mock_element6]

        mock_context.current_processing_time.return_value = timestamp

        results = list(extractor.process(mock_context, elements))

        expected_results = [[('user1', 'gold', timestamp), ('user2', 'gold', timestamp), ('user3', 'gold', timestamp), ('user4', 'silver', timestamp), ('user5', 'silver', timestamp), ('user6', 'silver', timestamp)]]
        self.assertEqual(expected_results, results)
