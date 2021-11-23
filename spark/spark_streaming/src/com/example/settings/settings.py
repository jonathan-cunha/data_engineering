

class Settings:
  spark = {
    "streaming_microbatch_duration": 10  # seconds
  }

  kafka = {
    "consumer": {
      "topics": ["test1"],
      "brokers": "192.168.0.18:9092"
    }
  }

