import logging
from pyflink.common import Types, Row
from pyflink.datastream import DataStream
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowSerializationSchema


class PublishData:

    def __init__(self, bootstrap_servers: str, kafka_topic: str):
        self.value_type_info = Types.ROW_NAMED(
            field_names=["data"],
            field_types=[Types.STRING()],
        )
        self.bootstrap_servers = bootstrap_servers
        self.kafka_topic = kafka_topic

    def publish_data_to_kafka_topic(self, datastream: DataStream):
        """
        Pushes the datastream elements to kafka topic by converting it to the string format
        :param datastream: Input datastream
        :return: None
        """
        sink = self.__create_kafka_sink()
        datastream.map(lambda e: Row(data=str(e)), output_type=self.value_type_info).sink_to(sink)
        logging.info("data has been pushed to Kafka topic...")

    def __create_kafka_sink(self):
        """
        Creates Kafka sink with the required properties
        :return: Returns Kafka sink
        """
        sink = KafkaSink.builder() \
            .set_bootstrap_servers(self.bootstrap_servers) \
            .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(self.kafka_topic)
            .set_value_serialization_schema(
                JsonRowSerializationSchema.builder().with_type_info(self.value_type_info).build()
            )
            .build()
        ) \
            .build()
        return sink
