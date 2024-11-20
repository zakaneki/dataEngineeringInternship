import os
import boto3
import requests
import json
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# Load environment variables from .env file
load_dotenv()

# API URL from environment variable
TOURIST_API_URL = os.getenv('TOURIST_API_URL')

def get_secret():
    secret_name = "tourist_estimate_token"
    region_name = "eu-west-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret_string = get_secret_value_response['SecretString']
    secret = json.loads(secret_string)
    return secret[secret_name]

def fetch_tourist_estimates(date):
    """
    Fetch tourist estimates for a given date from the API.

    Args:
        date (str): Date in the format 'YYYY-MM-DD'.

    Returns:
        dict: API response containing tourist data, or None if the request fails.
    """
    token = get_secret()
    headers = {
        'Authorization': f'Bearer {token}'
    }
    params = {
        'date': date
    }
    response = requests.get(TOURIST_API_URL, headers=headers, params=params)

    if response.status_code == 200:
        return response.json()  # Return the data if the request was successful
    else:
        print(f"Error: Unable to fetch data. Status code: {response.status_code}")
        return None

def get_city_tourist_estimate(city, date):
    """
    Get the estimated number of tourists for a specific city on a given date.

    Args:
        city (str): Name of the city to fetch estimates for.
        date (str): Date in the format 'YYYY-MM-DD'.

    Returns:
        int: Estimated number of tourists in the city, or None if not found.
    """
    data = fetch_tourist_estimates(date)

    if data and 'info' in data:
        for city_data in data['info']:
            if city_data['name'].lower() == city.lower():
                return city_data['estimated_no_people']
        print(f"City '{city}' not found in the API response.")
    else:
        print("No valid data received from the API.")
    return None
