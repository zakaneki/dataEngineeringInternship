import os
import boto3
import requests
import json
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# API URL and token (retrieved from environment variable)
TOURIST_API_URL = os.getenv('TOURIST_API_URL')

from botocore.exceptions import ClientError

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

# Make the API request to the tourist service
def fetch_tourist_estimates(date):
    token= get_secret()
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

def main():
    date = '2022-04-01'  # Use the desired date format YYYY-MM-DD

    # Fetch tourist estimates
    data = fetch_tourist_estimates(date)

    if data:
        print("Tourist Estimates:")
        for city in data['info']:
            print(f"City: {city['name']}, Estimated Visitors: {city['estimated_no_people']} thousand")
    else:
        print("No data received from the API.")

if __name__ == '__main__':
    main()
