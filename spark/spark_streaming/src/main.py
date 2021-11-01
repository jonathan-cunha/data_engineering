from com.example.app.streaming_app import StreamingApp
from com.example.handler.spark import Spark

Spark.start_streaming(StreamingApp().handler)
#StreamingApp().handler
