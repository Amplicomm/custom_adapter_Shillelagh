import urllib
from json import dump, dumps
from typing import Dict, List, Union, Tuple
from urllib import parse
from urllib.parse import urlparse, parse_qs

import requests
import requests_cache
from shillelagh.adapters.base import Adapter
from shillelagh.lib import analyze

QueryArg = Union[str, int]


def _parse_query_arg(k: str, v: List[str]) -> [str, [str, str]]:
    if len(v) > 1:
        raise ValueError(f"{k} was specified {len(v)} times")
    else:
        key_name_info = k.split('%')
        # print(key_name_info)
        if len(key_name_info) == 1:
            # return $ string, property name, value
            return '$', [key_name_info[0], v[0]]
        elif len(key_name_info) == 2:
            # return parent property name, property name, value
            return key_name_info[0], [key_name_info[1], v[0]]
        else:
            # TODO: Check the Different edge cases
            raise ValueError(f"{k} query format error")


def _parse_query_args(query: Dict[str, List[str]]) -> Dict[str, QueryArg]:
    payload_args = {}
    for key, value in query.items():
        payload_obj = _parse_query_arg(key[4:], value)
        if payload_obj[0] == '$':
            payload_args[payload_obj[1][0]] = payload_obj[1][1]
        else:
            dummy_payload = {payload_obj[1][0]: payload_obj[1][1]}
            if payload_args.get(payload_obj[0]) is None:
                payload_args[payload_obj[0]] = dummy_payload
            else:
                payload_args[payload_obj[0]][payload_obj[1][0]] = payload_obj[1][1]
    print(payload_args)
    return dict(payload_args)


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
        parsed = urlparse(uri)
        query_string = parse_qs(parsed.query)
        return (
                parsed.netloc == "3.108.177.44:9090"
                and parsed.path == "/filter-service/v1/analytics/filter-param-rawquery"
        )

    def __init__(
            self,
            table: str,
            query_args: Dict[str, QueryArg],
    ):
        super().__init__()
        self.table = table
        self.query_args = query_args

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
        query_args = _parse_query_args(query_string)
        # import pdb;pdb.set_trace()
        # calendar_month_year_str = query_string.get("calendar_month_year_str")[0]
        # Here we are targeting postId to be filterable
        return parsed.path, query_args

    def get_data(
            self,
            bounds,
            order,
            **kwargs,
    ):
        url = "http://3.108.177.44:9090/filter-service/v1/analytics/filter-param-rawquery"
        payload = self.query_args
        headers = {"Content-Type": "application/json"}
        response = requests.request("POST", url, json=payload, headers=headers)
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
