from pyspark.streaming import StreamingContext

from com.example.handler.kafka import KafkaConsumer


class StreamingHandler:
  @staticmethod
  def show(messages):
    records = messages.collect()
    for key, value in records:
      print(value)


class StreamingApp:
  @staticmethod
  def handler(ssc: StreamingContext):
    """ Main function that handles a streaming """

    kstream = KafkaConsumer.get_stream(ssc)  # creating kafka topic streaming
    kstream.foreachRDD(StreamingHandler.show)
    records = KafkaConsumer.get_records(kstream)  # getting records/messages from kafka topic streaming
    records.foreachRDD(
      lambda rdd: rdd.foreach(
        lambda msg: print(msg)
      )
    )
