from typing import Optional
from unittest import mock

import pytest
from prefect import flow
from transform.exceptions import QueryRuntimeException
from transform.models import MqlMaterializeResp, MqlQueryStatus, MqlQueryStatusResp

from prefect_transform.exceptions import (
    TransformAuthException,
    TransformConfigurationException,
    TransformRuntimeException,
)
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


def test_raises_on_connection_exception():
    @flow
    def test_flow():
        return create_materialization(
            api_key="key", mql_server_url="url", materialization_name="mt_name"
        )

    msg_match = "Cannot connect to Transform server!"
    with pytest.raises(TransformAuthException, match=msg_match):
        test_flow().result().result()


@mock.patch("prefect_transform.tasks.MQLClient")
def test_run_raises_on_create_materialization_async(mock_mql_client):
    error_msg = "Error while creating async materialization!"

    class MockMQLClient:
        def create_materialization(
            materialization_name: str,
            start_time: Optional[str] = None,
            end_time: Optional[str] = None,
            model_key_id: Optional[int] = None,
            output_table: Optional[str] = None,
            force: bool = False,
        ):
            return MqlQueryStatusResp(
                query_id="xyz",
                status=MqlQueryStatus.FAILED,
                sql="sql_query",
                error=error_msg,
                chart_value_max=None,
                chart_value_min=None,
                result=None,
                result_primary_time_granularity=None,
                result_source=None,
            )

    mock_mql_client.return_value = MockMQLClient

    @flow
    def test_flow():
        return create_materialization(
            api_key="key",
            mql_server_url="url",
            materialization_name="mt_name",
            wait_for_creation=False,
        )

    msg_match = (
        f"Transform materialization async creation failed! Error is: {error_msg}"
    )
    with pytest.raises(TransformRuntimeException, match=msg_match):
        test_flow().result().result()


@mock.patch("prefect_transform.tasks.MQLClient")
def test_run_raises_on_create_materialization_sync(mock_mql_client):
    error_msg = "Error while creating sync materialization!"

    class MockMQLClient:
        def materialize(
            materialization_name: str,
            start_time: Optional[str] = None,
            end_time: Optional[str] = None,
            model_key_id: Optional[int] = None,
            output_table: Optional[str] = None,
            force: bool = False,
            timeout: Optional[int] = None,
        ):
            raise QueryRuntimeException(query_id="xyz", msg=error_msg)

    mock_mql_client.return_value = MockMQLClient

    @flow
    def test_flow():
        return create_materialization(
            api_key="key", mql_server_url="url", materialization_name="mt_name"
        )

    msg_match = f"Transform materialization sync creation failed! Error is: {error_msg}"
    with pytest.raises(TransformRuntimeException, match=msg_match):
        test_flow().result().result()


@mock.patch("prefect_transform.tasks.MQLClient")
def test_run_on_create_materialization_async_successful_status(mock_mql_client):
    class MockMQLClient:
        def create_materialization(
            materialization_name: str,
            start_time: Optional[str] = None,
            end_time: Optional[str] = None,
            model_key_id: Optional[int] = None,
            output_table: Optional[str] = None,
            force: bool = False,
        ):
            return MqlQueryStatusResp(
                query_id="xyz",
                status=MqlQueryStatus.SUCCESSFUL,
                sql="sql_query",
                error=None,
                chart_value_max=None,
                chart_value_min=None,
                result=None,
                result_primary_time_granularity=None,
                result_source=None,
            )

    mock_mql_client.return_value = MockMQLClient

    @flow
    def test_flow():
        return create_materialization(
            api_key="key",
            mql_server_url="url",
            materialization_name="mt_name",
            wait_for_creation=False,
        )

    response = test_flow().result().result()

    assert response.is_complete is True
    assert response.is_successful is True
    assert response.is_failed is False


@mock.patch("prefect_transform.tasks.MQLClient")
def test_run_on_create_materialization_async_pending_status(mock_mql_client):
    class MockMQLClient:
        def create_materialization(
            materialization_name: str,
            start_time: Optional[str] = None,
            end_time: Optional[str] = None,
            model_key_id: Optional[int] = None,
            output_table: Optional[str] = None,
            force: bool = False,
        ):
            return MqlQueryStatusResp(
                query_id="xyz",
                status=MqlQueryStatus.PENDING,
                sql="sql_query",
                error=None,
                chart_value_max=None,
                chart_value_min=None,
                result=None,
                result_primary_time_granularity=None,
                result_source=None,
            )

    mock_mql_client.return_value = MockMQLClient

    @flow
    def test_flow():
        return create_materialization(
            api_key="key",
            mql_server_url="url",
            materialization_name="mt_name",
            wait_for_creation=False,
        )

    response = test_flow().result().result()

    assert response.is_complete is False
    assert response.is_successful is False
    assert response.is_failed is False


@mock.patch("prefect_transform.tasks.MQLClient")
def test_run_on_create_materialization_async_running_status(mock_mql_client):
    class MockMQLClient:
        def create_materialization(
            materialization_name: str,
            start_time: Optional[str] = None,
            end_time: Optional[str] = None,
            model_key_id: Optional[int] = None,
            output_table: Optional[str] = None,
            force: bool = False,
        ):
            return MqlQueryStatusResp(
                query_id="xyz",
                status=MqlQueryStatus.RUNNING,
                sql="sql_query",
                error=None,
                chart_value_max=None,
                chart_value_min=None,
                result=None,
                result_primary_time_granularity=None,
                result_source=None,
            )

    mock_mql_client.return_value = MockMQLClient

    @flow
    def test_flow():
        return create_materialization(
            api_key="key",
            mql_server_url="url",
            materialization_name="mt_name",
            wait_for_creation=False,
        )

    response = test_flow().result().result()

    assert response.is_complete is False
    assert response.is_successful is False
    assert response.is_failed is False


@mock.patch("prefect_transform.tasks.MQLClient")
def test_run_on_create_materialization_sync(mock_mql_client):
    class MockMQLClient:
        def materialize(
            materialization_name: str,
            start_time: Optional[str] = None,
            end_time: Optional[str] = None,
            model_key_id: Optional[int] = None,
            output_table: Optional[str] = None,
            force: bool = False,
            timeout: Optional[int] = None,
        ):
            return MqlMaterializeResp(schema="schema", table="table", query_id="xyz")

    mock_mql_client.return_value = MockMQLClient

    @flow
    def test_flow():
        return create_materialization(
            api_key="key", mql_server_url="url", materialization_name="mt_name"
        )

    response = test_flow().result().result()

    assert response.fully_qualified_name == "schema.table"
