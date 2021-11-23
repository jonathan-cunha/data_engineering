from pyspark.streaming.kafka import KafkaUtils, KafkaDStream

from com.example.settings.settings import Settings


class KafkaConsumer:
  @staticmethod
  def get_consumer_properties():
    return {
      "metadata.broker.list": Settings.kafka["consumer"]["brokers"]
    }

  @staticmethod
  def get_topics():
    return Settings.kafka["consumer"]["topics"]

  @staticmethod
  def get_stream(ssc):
    return KafkaUtils.createDirectStream(ssc, KafkaConsumer.get_topics(), KafkaConsumer.get_consumer_properties())

  @staticmethod
  def get_records(kstream: KafkaDStream):
    map_values_fn = lambda kv: (kv[1])
    return kstream.map(map_values_fn, preservesPartitioning=True)
