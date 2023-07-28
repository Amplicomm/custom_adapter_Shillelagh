import pytest
from shillelagh.fields import ISODate, ISODateTime, String

import adapter


def test_get_parse_query_arg_for_empty_input_keys_and_values():
    try:
        adapter._parse_query_arg('', [])
    except ValueError as exc:
        assert exc.args[0] == "Empty string passed"


def test_get_parse_query_arg_multiple_same_values():
    try:
        adapter._parse_query_arg('TEST_KEY_NAME', ['test_string', 'test_string'])
    except ValueError as exc:
        assert exc.args[0] == "TEST_KEY_NAME was specified 2 times"
