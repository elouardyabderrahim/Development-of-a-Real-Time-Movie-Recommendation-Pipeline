from kafka import KafkaProducer
import json
import requests
from config import KEY_API, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC  
import time

BASE_URL = "https://api.themoviedb.org/3/movie/popular"
LANGUAGE = "en-US"

def fetch_movie_data(page):
    MOVIE_ENDPOINT = f"{BASE_URL}?api_key={KEY_API}&language={LANGUAGE}&page={page}"
    response = requests.get(MOVIE_ENDPOINT)

    if response.status_code == 200:
        return response.json()['results']
    else:
        print(f"Error fetching data from TMDb API for page {page}")
        return []

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda k: str(k).encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

try:
    page_number = 1
    while True:
        movie_data = fetch_movie_data(page_number)
        print(f"Sending data for page {page_number} to Kafka")
        
        for movie in movie_data:
            producer.send(KAFKA_TOPIC, key=movie['id'], value=movie)

        page_number += 1
        time.sleep(2)  
except KeyboardInterrupt:
    print("Script interrupted. Closing the producer.")
    producer.close()
except Exception as e:
    print(f"An error occurred: {str(e)}")
    producer.close()
