from unittest import mock, TestCase
from services.group_and_filter import GroupAndFilter


class GroupAndFilterTest(TestCase):

    def setUp(self) -> None:
        self.test_data = [('user1', 'gold', '1234567890'), ('user2', 'gold', '1234567890'),
                          ('user3', 'gold', '1234567890'), ('user4', 'silver', '1234567890'),
                          ('user5', 'silver', '1234567890'), ('user6', 'silver', '1234567890')]

        self.process_function = GroupAndFilter()

    def test_process_element(self):
        with mock.patch('pyflink.datastream.ProcessFunction.Context') as mock_context:
            results = self.process_function.process_element(self.test_data, mock_context)

            expected_results = [[
                (('user1', 'gold'), ('user2', 'gold'), ('user3', 'gold')),
                (('user4', 'silver'), ('user5', 'silver'), ('user6', 'silver'))
            ]]

            self.assertEqual(list(results), expected_results)

