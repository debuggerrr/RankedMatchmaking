import unittest
from unittest.mock import MagicMock, patch
from services.extract_record_attributes import ExtractRecordAttributes


class TestExtractingRecordAttributes(unittest.TestCase):

    @patch('services.extract_record_attributes.ast.literal_eval')
    def test_process(self, mock_literal_eval):
        mock_context = MagicMock()
        mock_element1 = ('{"user": "user1", "rank": "rank1"}', 'timestamp1')
        mock_element2 = ('{"user": "user2", "rank": "rank2"}', 'timestamp2')

        mock_literal_eval.side_effect = lambda s: eval(s)

        extractor = ExtractRecordAttributes()

        key = "test_key"
        elements = [mock_element1, mock_element2]

        mock_context.current_processing_time.return_value = 1234567890

        results = list(extractor.process(key, mock_context, elements))

        expected_results = [('{"user": "user1", "rank": "rank1"}', 'timestamp1', '1234567890'),
                            ('{"user": "user2", "rank": "rank2"}', 'timestamp2', '1234567890')]
        self.assertEqual(results, expected_results)
