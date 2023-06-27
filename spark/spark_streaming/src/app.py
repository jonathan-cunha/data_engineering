import tempfile
from dataclasses import dataclass
from typing import Type

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, MapType, IntegerType


@dataclass
class Model:
    schema: StructType


class KafkaMessage:
    def __init__(self, df: DataFrame) -> None:
        self.data: DataFrame = df.selectExpr("value as message")


@dataclass
class WordNumber(Model):
    schema: StructType = StructType([StructField("word", StringType(), True),
                                     StructField("number", IntegerType(), True)])


@dataclass
class WordNumberSet:
    data: DataFrame


class WordNumberFactory:

    @staticmethod
    def create(msg: KafkaMessage) -> WordNumberSet:

        data = (msg.data
                .select(from_json(col("message"), schema=WordNumber.schema).alias("parsed_message"))
                .select(col("parsed_message.word"), col("parsed_message.number")))

        return WordNumberSet(data)


def handler(kafka_messages: KafkaMessage, ss: SparkSession) -> DataFrame:

    message: WordNumberSet = WordNumberFactory.create(kafka_messages)

    return message.data


def main():
    ss = SparkSession.builder.getOrCreate()

    with tempfile.TemporaryDirectory() as d:
        ss.createDataFrame([(10, '{"word": "a", "number": 1}'), (20, '{"word": "b", "number": 2}'), (30, '{"word": "c", "number": 3}')]).write.mode("overwrite").format("csv").save(d)

        stream: StreamingQuery = (ss
                                  .readStream
                                  .schema("key INT, value STRING")
                                  .format("csv")
                                  .load(d)
                                  .transform(lambda df: handler(KafkaMessage(df), ss))
                                  .writeStream
                                  .format("console")
                                  .outputMode("append")
                                  .start())

        stream.awaitTermination()

    ss.stop()


if __name__ == "__main__":
    main()
