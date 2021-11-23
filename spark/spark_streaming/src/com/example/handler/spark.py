from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

from com.example.settings.settings import Settings


class Spark:
  ss: SparkSession = (SparkSession
                      .builder
                      .master("local[*]")
                      .appName("Spark Demo")
                      .getOrCreate())
  ss.sparkContext.setLogLevel("WARN")
  ssc: StreamingContext = StreamingContext(ss.sparkContext, Settings.spark["streaming_microbatch_duration"])

  @staticmethod
  def get_streaming_context():
    return Spark.ssc, Spark.ss

  @staticmethod
  def start_streaming(handler):
    ssc, ss = Spark.get_streaming_context()
    handler(ssc)
    ssc.start()
    ssc.awaitTermination()

