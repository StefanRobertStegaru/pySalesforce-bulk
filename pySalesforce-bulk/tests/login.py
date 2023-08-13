import requests


def salesforce_login(username, password, domain, client_id, client_secret):
    auth_url = f'https://login.{domain}/services/oauth2/token'
    params = {
        "grant_type": "Password",
        "client_id": client_id,
        "client_secret": client_secret,
        "Username":	username,
        "Password": password
    }

    auth_response = requests.post(url=auth_url, params=params)

    if auth_response.status_code == 200:
        auth_data = auth_response.json()  # Convert response content to JSON
        # Extract the 'signature' field
        signature = auth_data.get("signature")
        # Extract the 'signature' field
        instance_url = auth_data.get("instance_url")

    headers = {
        "Authorization": f"Bearer {signature}"
    }

    return (
        headers, instance_url
    )
