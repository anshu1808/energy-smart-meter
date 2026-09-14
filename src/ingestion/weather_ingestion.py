import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import os


def fetch_weather_data():
    """
    Fetches weather data from a weather API for a given location.

    Args:
        api_key (str): The API key for authentication.
        location (str): The location for which to fetch weather data.

    Returns:
        dict: The weather data for the specified location.
    """
    url = f"https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print(response.status_code)
    except requests.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return {}
    os.makedirs("datasets/weather", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(f"datasets/weather/weather_data_{timestamp}.json", "w") as f:
        json.dump(response.json(), f, indent=4)
    print(f"Weather data saved to datasets/weather/weather_data_{timestamp}.json")
    #print(response.status_code)
    #print(response.text[:500])
    data = response.json()
    return data


if __name__ == "__main__":
    weather_data = fetch_weather_data()
    # Convert the weather data to a DataFrame for further processing if needed
    df = pd.DataFrame(weather_data)
    print(df.head())