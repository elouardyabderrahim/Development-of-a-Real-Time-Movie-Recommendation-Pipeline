from kafka import KafkaProducer
import json
import requests
from config import KEY_API_bd  # Import API key from config

API_KEY = KEY_API_bd
BASE_URL = "https://api.themoviedb.org/3/movie/popular"
LANGUAGE = "en-US"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'movie_data_topic'

def fetch_movie_data(page):
    MOVIE_ENDPOINT = f"{BASE_URL}?api_key={API_KEY}&language={LANGUAGE}&page={page}"
    response = requests.get(MOVIE_ENDPOINT)

    if response.status_code == 200:
        return response.json()['results']
    else:
        print(f"Error fetching data from TMDb API for page {page}")
        return []

# Number of pages you want to retrieve
num_pages = 100
all_movie_data = []

for page_number in range(1, num_pages + 1):
    movie_data = fetch_movie_data(page_number)
    all_movie_data.extend(movie_data)

# Create Kafka producer instance
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Produce messages to Kafka topic
for movie in all_movie_data:
    # Produce the message to the Kafka topic
    producer.send(KAFKA_TOPIC, key=str(movie['id']), value=movie)

# Close the producer
producer.close()

print(f"Data for {num_pages} pages has been successfully sent to Kafka topic {KAFKA_TOPIC}.")