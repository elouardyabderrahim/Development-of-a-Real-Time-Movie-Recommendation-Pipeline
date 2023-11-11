from kafka import KafkaConsumer
import json
from config import KAFKA_TOPIC,KAFKA_BOOTSTRAP_SERVERS


consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='latest',
    group_id='movie_data_consumer',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    movie_data = message.value
    print(f"Received message: {movie_data}")

consumer.close()