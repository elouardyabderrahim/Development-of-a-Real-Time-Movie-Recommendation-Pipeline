import requests
import csv

API_KEY = 'ef9921191c448632e53ff5ee5e5501d9'
BASE_URL = "https://api.themoviedb.org/3/movie/popular"
LANGUAGE = "en-US"

def fetch_movie_data(page):
    MOVIE_ENDPOINT = f"{BASE_URL}?api_key={API_KEY}&language={LANGUAGE}&page={page}"
    response = requests.get(MOVIE_ENDPOINT)

    if response.status_code == 200:
        return response.json()['results']
    else:
        print(f"Error fetching data from TMDb API for page {page}")
        return []

# Number of pages you want to retrieve
num_pages = 5
all_movie_data = []

for page_number in range(1, num_pages + 1):
    movie_data = fetch_movie_data(page_number)
    all_movie_data.extend(movie_data)

# Extract all possible headers dynamically
all_headers = set()
for movie in all_movie_data:
    all_headers.update(movie.keys())

# Save the data to a CSV file
csv_filename = 'movie_data.csv'

with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=list(all_headers))
    
    # Write the header
    writer.writeheader()
    
    # Write the data
    for movie in all_movie_data:
        writer.writerow(movie)

print(f"Data for {num_pages} pages has been successfully retrieved and stored in {csv_filename}.")
