from typing import Iterable, Tuple
from pyflink.datastream.functions import ProcessAllWindowFunction
from model.model import UserData
import ast
import re
import datetime
import logging


class ExtractRecordAttributes(ProcessAllWindowFunction):

    def __init__(self):
        pass

    def process(self, context: 'ProcessAllWindowFunction.Context',
                elements: Iterable[Tuple[str, str]]) -> Iterable[Tuple[str, str, str]]:
        """
        This method will group the tuple elements of a specific window in a list

        :param context: The context holding window metadata
        :param elements: Input elements
        :return: List of tuples for a specific window frame.
        """
        result_list = []
        for element in elements:
            parts = UserData(*ast.literal_eval(str(element)))
            result = (parts.user, re.sub(r'\d+', '', parts.rank), str(self.__get_epoch_timestamp()))
            result_list.append(result)
        logging.info(f"data extracted as {str(result_list)}")
        yield result_list

    def clear(self, context: 'ProcessAllWindowFunction.Context'):
        pass

    def __get_epoch_timestamp(self):
        now = datetime.datetime.now()
        return int(now.timestamp())
