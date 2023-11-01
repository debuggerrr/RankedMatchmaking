from kafka.consume_data import ConsumeData
from pyflink.datastream import StreamExecutionEnvironment
from kafka.data_processor import DataProcessor
from services.extract_record_attributes import ExtractRecordAttributes
from services.group_and_filter import GroupAndFilter
from kafka.publish_data import PublishData
from pathlib import Path
import os

if __name__ == "__main__":
    library_directory = os.path.join(Path(os.path.dirname(os.path.realpath(__file__))).parent.parent, "lib")
    env = StreamExecutionEnvironment.get_execution_environment()
    source_data = ConsumeData(env, "localhost:9092", "test-topic2", jarfile="file://" + library_directory + "/flink-sql-connector-kafka-1.17.1.jar")
    ds = source_data.get_kafka_data()
    elements = DataProcessor(ExtractRecordAttributes(), GroupAndFilter()).get_data_stream(ds)
    PublishData("localhost:9092", "test-topic45").publish_data_to_kafka_topic(elements)
    env.execute("source")
