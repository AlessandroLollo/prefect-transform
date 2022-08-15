from unittest import mock

import pytest
from pydantic import SecretStr

from prefect_transform.credentials import TransformCredentials
from prefect_transform.exceptions import (
    TransformAuthException,
    TransformConfigurationException,
)


def test_credentials_construction():
    credentials = TransformCredentials(api_key=SecretStr("foo"), mql_server_url="foo")

    assert credentials.api_key == SecretStr("foo")
    assert credentials.mql_server_url == "foo"


def test_missing_api_key_api_key_env_var_raises():
    msg_match = "Both `api_key` and `api_key_env_var` are missing."
    with pytest.raises(TransformConfigurationException, match=msg_match):
        TransformCredentials()


def test_api_key_env_var_not_found_raises():
    msg_match = "`api_key` is missing and `api_key_env_var` not found in env vars."
    with pytest.raises(TransformConfigurationException, match=msg_match):
        TransformCredentials(api_key_env_var="foo")


def test_missing_mql_server_url_mql_server_url_env_var_raises():
    msg_match = "Both `mql_server_url` and `mql_server_url_env_var` are missing."
    with pytest.raises(TransformConfigurationException, match=msg_match):
        TransformCredentials(api_key=SecretStr("foo"))


def test_mql_server_url_env_var_not_found_raises():
    msg_match = """
            `mql_server_url` is missing and `mql_server_url_env_var`
            not found in env vars.
    """
    with pytest.raises(TransformConfigurationException, match=msg_match):
        TransformCredentials(api_key=SecretStr("foo"), mql_server_url_env_var="foo")


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
