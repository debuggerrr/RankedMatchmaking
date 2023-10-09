from kafka.consume_data import ConsumeData
from pyflink.datastream import StreamExecutionEnvironment
from kafka.data_processor import DataProcessor
from services.extract_record_attributes import ExtractRecordAttributes

if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    source_data = ConsumeData(env, jarfile="file:///Users/sid/Downloads/flink-sql-connector-kafka-1.17.1.jar")
    ds = source_data.get_kafka_data()
    DataProcessor(ExtractRecordAttributes()).get_data_stream(ds).print()
    env.execute("source")
