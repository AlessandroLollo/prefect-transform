import pytest
from prefect import flow

from prefect_transform.exceptions import TransformConfigurationException
from prefect_transform.tasks import create_materialization


def test_missing_api_key_api_key_env_var_raises():
    @flow
    def test_flow():
        return create_materialization()

    msg_match = "Both `api_key` and `api_key_env_var` are missing."
    with pytest.raises(TransformConfigurationException, match=msg_match):
        test_flow().result().result()


def test_api_key_env_var_not_found_raises():
    @flow
    def test_flow():
        return create_materialization(api_key_env_var="env_var")

    msg_match = "`api_key` is missing and `api_key_env_var` not found in env vars."
    with pytest.raises(TransformConfigurationException, match=msg_match):
        test_flow().result().result()


def test_missing_mql_server_url_mql_server_url_env_var_raises():
    @flow
    def test_flow():
        return create_materialization(api_key="key")

    msg_match = "Both `mql_server_url` and `mql_server_url_env_var` are missing."
    with pytest.raises(TransformConfigurationException, match=msg_match):
        test_flow().result().result()


def test_mql_server_url_env_var_not_found_raises():
    @flow
    def test_flow():
        return create_materialization(api_key="key", mql_server_url_env_var="env_var")

    msg_match = """
        `mql_server_url` is missing and `mql_server_url_env_var` not found in env vars.
    """
    with pytest.raises(TransformConfigurationException, match=msg_match):
        test_flow().result().result()


def test_missing_materialization_name_raises():
    @flow
    def test_flow():
        return create_materialization(api_key="key", mql_server_url="url")

    msg_match = "`materialization_name` is missing."
    with pytest.raises(TransformConfigurationException, match=msg_match):
        test_flow().result().result()
