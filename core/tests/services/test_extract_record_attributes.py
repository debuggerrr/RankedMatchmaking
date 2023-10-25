import unittest
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

        mock_literal_eval.side_effect = lambda s: eval(s)

        extractor = ExtractRecordAttributes()

        key = "test_key"
        elements = [mock_element1, mock_element2, mock_element3, mock_element4, mock_element5, mock_element6]

        mock_context.current_processing_time.return_value = 1234567890

        results = list(extractor.process(key, mock_context, elements))

        expected_results = [[('user1', 'gold', '1234567890'), ('user2', 'gold', '1234567890'), ('user3', 'gold', '1234567890'), ('user4', 'silver', '1234567890'), ('user5', 'silver', '1234567890'), ('user6', 'silver', '1234567890')]]
        self.assertEqual(expected_results, results)
