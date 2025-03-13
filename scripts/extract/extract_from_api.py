import requests
import time
from typing import Dict

def extract_from_api(url: str, headers: dict = None, method: str = 'GET', params: dict = None, data: dict = None, max_retries: int = 5) -> Dict:
    """
    Performs an HTTP request and returns the JSON response or raw text.

    Args:
        url (str): The API endpoint URL.
        headers (dict, optional): Additional headers for the request.
        method (str, optional): HTTP method ('GET' or 'POST'). Defaults to 'GET'.
        params (dict, optional): Query parameters for GET requests.
        data (dict or list, optional): JSON payload for POST requests.
        max_retries (int, optional): Maximum number of retry attempts in case of rate limit errors. Defaults to 5.

    Returns:
        dict or str: JSON response from the API or raw text if JSON parsing fails.

    Raises:
        RuntimeError: If an HTTP error occurs after the maximum number of retries.
    """
    print(f"Accessing URL: {url}")
    retries = 0

    while retries < max_retries:
        try:
            response = requests.request(method, url, headers=headers, params=params, json=data)
            response.raise_for_status()
            
            try:
                return response.json()
            
            except requests.exceptions.JSONDecodeError:

                print("Response is not JSON. Returning as plain text.")
                return response.text

        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 2))

                print(f"Rate limit reached. Retrying after {retry_after} seconds...")

                time.sleep(retry_after)
                retries += 1
            else:
                print(f"HTTP error {response.status_code}: {e}")
                raise

    raise RuntimeError(f"Max retries reached ({max_retries}) while accessing {url}.")
