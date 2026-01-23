import requests
import logging
from .access_secrets import *

J_CLIENT_ID = access_secret_version('icef-437920', 'illuminate_client_id', version_id="latest")
J_CLIENT_SECRET = access_secret_version('icef-437920', 'illuminate_access_key', version_id="latest")
token_url_illuminate = 'https://icefps.illuminateed.com/live/'
base_url_illuminate = 'https://icefps.illuminateed.com/live/rest_server.php/Api/'

# Configuration
TOKEN_URL = f'{token_url_illuminate}?OAuth2_AccessToken'
TOKEN_EXPIRY = 3600  # Token expiry time in seconds (1 hour)

def get_access_token():
    # Prepare the payload for the token request
    payload = {
        'client_id': J_CLIENT_ID,
        'client_secret': J_CLIENT_SECRET,
        'grant_type': 'client_credentials',  # Assuming client credentials grant type
    }


    # print(f'Here is the client ID, and here is the Client Secert {J_CLIENT_ID}, {J_CLIENT_SECRET}')
    
    # Request the access token
    try:
        response = requests.post(TOKEN_URL, data=payload)
        logging.info('Calling API token endpoint')
    except Exception as e:
        logging.error(f'Unable to get API token succesfully due to {e}')
        raise Exception(f"Error occurred while getting API token: {e}")
    
    if response.status_code == 200:
        logging.info('Succesfully retrieved API token')
        token_info = response.json()
        access_token = token_info.get('access_token')
        expires_in = token_info.get('expires_in', TOKEN_EXPIRY)  # Default to 1 hour if not provided
        
        return access_token, expires_in
    else:
        logging.error(f'Failed to obtain access token: {response.status_code} {response.text}')
        print('Failed to obtain access token:', response.status_code, response.text)
        return None, None
        