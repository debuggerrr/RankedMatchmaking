from typing import Iterable, Tuple
from pyflink.datastream import ProcessWindowFunction
from model.model import UserData
import ast


class ExtractRecordAttributes(ProcessWindowFunction):

    def __init__(self):
        pass

    def process(self, key: str,
                context: 'ProcessWindowFunction.Context',
                elements: Iterable[Tuple[str, str]]) -> Iterable[Tuple[str, str, str]]:
        result = ""
        for element in elements:
            parts = UserData(*ast.literal_eval(str(element)))
            result = (parts.user, parts.rank, str(context.current_processing_time()))
            yield result

    def clear(self, context: 'ProcessWindowFunction.Context'):
        pass