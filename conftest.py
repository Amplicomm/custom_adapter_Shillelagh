from typing import Final, Generator

import pytest
import responses
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine

API_URL: Final[
    str
] = "http://3.108.177.44:9090/filter-service/v1/analytics/filter-param-rawquery"


@pytest.fixture
def swapi_api_url() -> str:
    return API_URL


@pytest.fixture
def swapi_engine(api_url: str) -> Engine:
    return create_engine(api_url)


@pytest.fixture
def mocked_responses() -> Generator[responses.RequestsMock, None, None]:
    with responses.RequestsMock() as rsps:
        yield rsps
