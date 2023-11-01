import logging
from collections import defaultdict
from pyflink.datastream import ProcessFunction


class GroupAndFilter(ProcessFunction):

    def __init__(self):
        pass

    def process_element(self, value, ctx: 'ProcessFunction.Context'):
        """
        This method will group the similar ranked elements together in a set of 3 tuples per set.

        :param value: Input elements
        :param ctx: The context holding metadata
        :return: list of tuple of tuple with similar ranked elements grouped together.
        """
        grouped_data = defaultdict(list)
        for user, rank, _ in value:
            grouped_data[rank].append((user, rank))
        result = []
        for rank, users in grouped_data.items():
            for i in range(0, len(users), 3):
                result.append(tuple(users[i:i+3]))
        logging.info(f"data formed in groups as {result}")
        yield result

