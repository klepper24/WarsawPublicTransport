import settings


def generate_api_warszawa_url(api_key: str, resource_id: str, **kwargs):
    """
    Returns url to Warsaw Public Transport API with given parameters as query strings
    """
    url = settings.API_WARSZAWA_URL
    url += f'?apikey={api_key}&resource_id={resource_id}'
    for k, v in kwargs.items():
        url += f'&{k}={v}'
    return url
