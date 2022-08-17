from unittest import mock

import pytest
from pydantic import SecretStr

from prefect_transform.credentials import TransformCredentials
from prefect_transform.exceptions import TransformAuthException


def test_credentials_construction():
    credentials = TransformCredentials(api_key=SecretStr("foo"), mql_server_url="foo")

    assert credentials.api_key == SecretStr("foo")
    assert credentials.mql_server_url == "foo"


@mock.patch("prefect_transform.credentials.MQLClient")
def test_mql_client_creation_raises_auth_exception(mock_mql_client):
    class MockMQLClient:
        def __init__(self):
            super().__init__()

    mock_mql_client.return_value = MockMQLClient

    msg_match = "Cannot connect to Transform server!"
    mock_mql_client.side_effect = TransformAuthException(msg_match)

    with pytest.raises(TransformAuthException, match=msg_match):
        TransformCredentials(
            api_key=SecretStr("foo"), mql_server_url="foo"
        ).get_client()


@mock.patch("prefect_transform.credentials.MQLClient")
def test_mql_client_creation_succeed(mock_mql_client):
    class MockMQLClient:
        def __init__(self):
            super().__init__()

        def a_method(self):
            pass

    mock_mql_client.return_value = MockMQLClient

    mql_client = TransformCredentials(
        api_key=SecretStr("foo"), mql_server_url="foo"
    ).get_client()

    assert hasattr(mql_client, "a_method")
