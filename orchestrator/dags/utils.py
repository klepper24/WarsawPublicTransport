import settings


def generate_api_warszawa_url(endpoint: str, api_key: str, resource_id: str, resource: str, **kwargs) -> str:
    """
    Returns url to Warsaw Public Transport API with given parameters as query strings
    """
    url = settings.API_WARSZAWA_URL + endpoint
    url += f'?{resource}={resource_id}&apikey={api_key}'
    for k, v in kwargs.items():
        url += f'&{k}={v}'
    return url
