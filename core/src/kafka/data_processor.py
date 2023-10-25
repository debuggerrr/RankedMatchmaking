from pyflink.common import Time, Types
from pyflink.datastream import DataStream, WindowedStream
from pyflink.datastream.window import TumblingProcessingTimeWindows
from services.extract_record_attributes import ExtractRecordAttributes
from services.group_and_filter import GroupAndFilter


class DataProcessor:

    def __init__(self, extract_record_attributes: ExtractRecordAttributes, group_and_filter: GroupAndFilter):
        self.extract_record_attributes = extract_record_attributes
        self.group_and_filter = group_and_filter

    def get_data_stream(self, data_stream: DataStream):
        """
        This method will accept the elements per 5 seconds window.

        :param data_stream: It will accept the datastream
        :return: It will print the datastream
        """
        result_stream = data_stream.key_by(lambda x: x[0]).window(
            TumblingProcessingTimeWindows.of(Time.seconds(5))
        )
        return self.__process_data_stream(result_stream)

    def __process_data_stream(self, data_stream: WindowedStream):
        """
        This method will apply Process Window Function to the data stream elements and will return the required data
        type i.e Tuple

        :param data_stream: input datastream
        :return: List of List of Tuple data
        """
        result_stream = data_stream.process(
            self.extract_record_attributes,
            output_type=Types.LIST(Types.TUPLE([Types.STRING(), Types.STRING(), Types.STRING()]))
        ).process(GroupAndFilter()).uid("extract_tuple")
        return result_stream