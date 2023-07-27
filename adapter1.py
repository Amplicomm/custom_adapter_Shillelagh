import urllib

import requests
import requests_cache
from shillelagh.adapters.base import Adapter
from shillelagh.lib import analyze


class CustomJsonAPI(Adapter):
    """
    An adapter to data from http://3.108.177.44:9090/.
    """

    safe = True

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs):
        """
        Method which checks whether given uri could be handled by the adapter
        """
        parsed = urllib.parse.urlparse(uri)
        query_string = urllib.parse.parse_qs(parsed.query)
        return (
                parsed.netloc == "3.108.177.44:9090"
                and parsed.path == "/filter-service/v1/analytics/filter-param-rawquery"
        )

    def __init__(self):
        super().__init__()

        # self.calendar_month_year_str = calendar_month_year_str
        # using cache, since the adapter does a lot of similar API requests and the data rarely changes
        self._session = requests_cache.CachedSession(
            cache_name="jsonplaceholders_cache",
            backend="sqlite",
            expire_after=180,
        )
        # Set columns based on the result
        self._set_columns()

    @staticmethod
    def parse_uri(uri: str):
        parsed = urllib.parse.urlparse(uri)
        query_string = urllib.parse.parse_qs(parsed.query)

        # import pdb;pdb.set_trace()
        # calendar_month_year_str = query_string.get("calendar_month_year_str")[0]
        # Here we are targetting postId to be filterable
        return ()

    # @staticmethod
    # def parse_uri(uri: str):
    #     return uri

    # def __init__(self, uri: str):
    #     """
    #     Instantiate the adapter.

    #     Here ``uri`` will be passed from the ``parse_uri`` method
    #     """
    #     super().__init__()

    #     parsed = urllib.parse.urlparse(uri)
    #     query_string = urllib.parse.parse_qs(parsed.query)

    #     self.postId = query_string["postId"][0]
    #     self._session = requests_cache.CachedSession(
    #         cache_name="jsonplaceholders_cache",
    #         backend="sqlite",
    #         expire_after=180,
    #         )

    def get_data(
            self,
            bounds,
            order,
            **kwargs,
    ):
        # url = "https://jsonplaceholder.typicode.com/comments"
        # params = {"calendar_month_year_str": self.calendar_month_year_str}
        # response = self._session.get(url, params=params)
        # if response.ok:
        #     return response.json()

        url = "http://3.108.177.44:9090/filter-service/v1/analytics/filter-param-rawquery"

        querystring = {"baseQueryName": "gmv_monthly_financial_yr_v1"}
        # data = parse.urlencode({"baseQueryName":"gmv_monthly_financial_yr_v1"}).encode()
        payload = {"baseQueryName": "gmv_monthly_financial_yr_v1"}
        headers = {"Content-Type": "application/json"}

        response = requests.request("POST", url, json=payload, headers=headers, params=querystring)

        # print(response.text)
        return response.json()['reportData']

    def _set_columns(self):
        rows = self.get_data({}, [])
        column_names = rows[0].keys() if rows else []

        _, order, types = analyze(iter(rows))

        self.columns = {
            column_name: types[column_name](
                filters=[],
                order=order[column_name],
                exact=False,
            )
            for column_name in column_names
        }

    def get_columns(self):
        return self.columns
