import settings
import requests


class WarsawApi:
    BASE_URL = 'https://api.um.warszawa.pl/'
    ENDPOINT_DBSTORE = 'api/action/dbstore_get/'
    ENDPOINT_BUSESTRAMS = 'api/action/busestrams_get/'

    def __init__(self, apikey):
        self._client = requests.session()
        self._apikey = apikey

    def _get(self, url, params=None):
        if params is None:
            params = {}
        default_params = {"apikey": self._apikey}
        default_params.update(params)
        response = self._client.get(
            f"{WarsawApi.BASE_URL}{url}", params=default_params
        )
        response.raise_for_status()
        return response.json()

    def get_dbstore(self, resource_id=None):
        params = {}
        if resource_id is not None:
            params["id"] = resource_id
        self._dbstore = self._get(WarsawApi.ENDPOINT_DBSTORE, params)
        return self._dbstore

    def get_busestrams(self, resource_id=None, resource_type=None):
        params = {}
        if resource_id is not None and resource_type is not None:
            params["resource_id"] = resource_id
            params["type"] = resource_type
        return self._get(WarsawApi.ENDPOINT_BUSESTRAMS, params)