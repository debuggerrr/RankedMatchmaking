import logging
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.stream_execution_environment import RuntimeExecutionMode
from pyflink.common import SimpleStringSchema, WatermarkStrategy


class ConsumeData:
    def __init__(self, env, bootstrap_servers: str, kafka_topic: str, jarfile=None):
        self.env = env
        self.bootstrap_servers = bootstrap_servers
        self.kafka_topic = kafka_topic
        self.__configure_environment(jarfile)

    def get_kafka_data(self):
        """
        This method is like caller which call the respective operations in order to print the datastream as the end result
        :return: It will print the stream
        """
        source = self.__create_kafka_source()
        ds = self.__create_data_stream(source)
        logging.info("datastream has been created...")
        return ds

    def __configure_environment(self, jarfile=None):
        """
        This method will set the configuration environment for the flink application

        :param jarfile: location of the required jarfile
        :return: It will not return anything
        """
        if jarfile:
            self.env.add_jars(jarfile)
        self.env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
        self.env.set_parallelism(1)

    def __create_kafka_source(self):
        """
        This method will create and return the KafkaSource object which has the required kafka properties

        :return: It will return the KafkaSource object.
        """
        return KafkaSource.builder() \
            .set_bootstrap_servers(self.bootstrap_servers) \
            .set_topics(self.kafka_topic) \
            .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()

    def __create_data_stream(self, source: KafkaSource):
        """
        This method will accept the KafkaSource object and will fetch the data from Kafka and create a datastream out
        of it.

        :param source: KafkaSource object
        :return: datastream
        """
        return self.env.from_source(
            source,
            WatermarkStrategy.no_watermarks(),
            "Kafka Source"
        )
