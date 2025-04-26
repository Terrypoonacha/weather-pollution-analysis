import requests
import json
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

API_KEY = 'dcc5e4a95e415de8a13ddefd6884d19a'
CITY = "Stuttgart"

url = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"

def fetch_weather():
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        with open("data/weather_data.json", "w") as f:
            json.dump(data, f, indent=4)
        print(f"Weather data fetched and saved for {CITY} ✅")
    else:
        print(f"Failed to fetch weather data: {response.status_code}")

if __name__ == "__main__":
    fetch_weather()
