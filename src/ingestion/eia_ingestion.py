import requests
import json
import pandas as pd
from datetime import datetime
import os


EIA_API_KEY = "jFXug8kSuVUDaTAwhgay5dFMQTeadLzf4Ig1hPbX"

def fetch_eia_data():
    """
    Fetches EIA data from the EIA API.

    Args:
        api_key (str): The API key for authentication.

    Returns:
        dict: The EIA data.
    """
    url = f"https://api.eia.gov/v2/electricity/rto/region-data/data/?frequency=hourly&data[0]=value&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000&api_key={EIA_API_KEY}"
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()  # Raise an exception for HTTP errors
    except requests.RequestException as e:
        print(f"Error fetching EIA data: {e}")
        return {}
    os.makedirs("datasets/eia", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(f"datasets/eia/eia_data_{timestamp}.json", "w") as f:
        json.dump(response.json(), f, indent=4)
    print(f"EIA data saved to datasets/eia/eia_data_{timestamp}.json")
    #print(response.status_code)
    #print(response.text[:500])
    data = response.json()
    print(data.keys())
    print(data["response"].keys())
    return data


if __name__ == "__main__":
    eia_data = fetch_eia_data()
    # Convert the EIA data to a DataFrame for further processing if needed
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    df = pd.DataFrame(eia_data["response"]["data"])
    print(df.head())
    df.to_csv(f"datasets/eia/eia_data_{timestamp}.csv", index=False)