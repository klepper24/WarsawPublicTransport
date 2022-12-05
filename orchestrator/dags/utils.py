import settings
import requests


class ApiWarsawUrl:
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
            f"{ApiWarsawUrl.BASE_URL}{url}", params=default_params
        )
        response.raise_for_status()
        return response.status_code, response.json()

    def get_dbstore(self, id=None):
        params = {}
        if id is not None:
            params["id"] = id
        self._dbstore = self._get(ApiWarsawUrl.ENDPOINT_DBSTORE, params)
        return self._dbstore

    def get_busestrams(self, resource_id=None, type=None):
        params = {}
        if resource_id is not None and type is not None:
            params["resource_id"] = resource_id
            params["type"] = type
        self._busestrams = self._get(ApiWarsawUrl.ENDPOINT_BUSESTRAMS, params)
        return self._busestrams