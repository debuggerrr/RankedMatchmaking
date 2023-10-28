import json
from pyflink.common import Types, Row
from pyflink.datastream import DataStream
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowSerializationSchema


class PublishData:

    def __init__(self):
        self.value_type_info = Types.ROW_NAMED(
            field_names=["data"],
            field_types=[Types.STRING()],
        )

    def publish_data_to_kafka_topic(self, datastream: DataStream):
        sink = self.__create_kafka_sink()
        datastream.map(lambda e: Row(data=json.dumps(e)), output_type=self.value_type_info).sink_to(sink)

    def __create_kafka_sink(self):
        sink = KafkaSink.builder() \
            .set_bootstrap_servers("localhost:9092") \
            .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("test-topic45")
            .set_value_serialization_schema(
                JsonRowSerializationSchema.builder().with_type_info(self.value_type_info).build()
            )
            .build()
        ) \
            .build()
        return sink
