source ./venv/bin/activate
zip -r lib/dependencies.zip src/com
spark-submit --packages org.apache.spark:spark-avro_2.11:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0,org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.7 --py-files lib/dependencies.zip src/main.py
deactivate