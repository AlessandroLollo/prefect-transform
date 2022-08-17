from typing import List, Optional
from unittest import mock

import pytest
from prefect import flow
from pydantic import SecretStr
from transform.exceptions import QueryRuntimeException
from transform.models import MqlMaterializeResp, MqlQueryStatus, MqlQueryStatusResp

from prefect_transform.credentials import TransformCredentials
from prefect_transform.exceptions import TransformRuntimeException
from prefect_transform.tasks import create_materialization


class MockTransformCredentials:
    def get_client(self):
        pass


@mock.patch("prefect_transform.credentials.TransformCredentials")
@mock.patch("prefect_transform.credentials.MQLClient")
def test_run_raises_on_create_materialization_async(
    mock_mql_client, mock_transform_credentials
):
    error_msg = "Error while creating async materialization!"

    class MockMQLClient:
        def create_materialization(
            materialization_name: str,
            start_time: Optional[str] = None,
            end_time: Optional[str] = None,
            model_key_id: Optional[int] = None,
            output_table: Optional[str] = None,
            force: bool = False,
            warnings: List[str] = None,
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
                warnings=[],
            )

    mock_mql_client.return_value = MockMQLClient

    mock_transform_credentials.return_value = MockTransformCredentials
    mock_transform_credentials.get_client.return_value = mock_mql_client

    @flow(name="test_flow_7")
    def test_flow():
        return create_materialization(
            credentials=TransformCredentials(
                api_key=SecretStr("foo"), mql_server_url="foo"
            ),
            materialization_name="mt_name",
            wait_for_creation=False,
        )

    msg_match = (
        f"Transform materialization async creation failed! Error is: {error_msg}"
    )
    with pytest.raises(TransformRuntimeException, match=msg_match):
        test_flow()


@mock.patch("prefect_transform.credentials.TransformCredentials")
@mock.patch("prefect_transform.credentials.MQLClient")
def test_run_raises_on_create_materialization_sync(
    mock_mql_client, mock_transform_credentials
):
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

    mock_transform_credentials.return_value = MockTransformCredentials
    mock_transform_credentials.get_client.return_value = mock_mql_client

    @flow(name="test_flow_8")
    def test_flow():
        return create_materialization(
            credentials=TransformCredentials(
                api_key=SecretStr("foo"), mql_server_url="foo"
            ),
            materialization_name="mt_name",
        )

    msg_match = f"Transform materialization sync creation failed! Error is: {error_msg}"
    with pytest.raises(TransformRuntimeException, match=msg_match):
        test_flow()


@mock.patch("prefect_transform.credentials.TransformCredentials")
@mock.patch("prefect_transform.credentials.MQLClient")
def test_run_on_create_materialization_async_successful_status(
    mock_mql_client, mock_transform_credentials
):
    class MockMQLClient:
        def create_materialization(
            materialization_name: str,
            start_time: Optional[str] = None,
            end_time: Optional[str] = None,
            model_key_id: Optional[int] = None,
            output_table: Optional[str] = None,
            force: bool = False,
            warnings: List[str] = None,
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
                warnings=[],
            )

    mock_mql_client.return_value = MockMQLClient

    mock_transform_credentials.return_value = MockTransformCredentials
    mock_transform_credentials.get_client.return_value = mock_mql_client

    @flow(name="test_flow_9")
    def test_flow():
        return create_materialization(
            credentials=TransformCredentials(
                api_key=SecretStr("foo"), mql_server_url="foo"
            ),
            materialization_name="mt_name",
            wait_for_creation=False,
        )

    response = test_flow()

    assert response.is_complete is True
    assert response.is_successful is True
    assert response.is_failed is False


@mock.patch("prefect_transform.credentials.TransformCredentials")
@mock.patch("prefect_transform.credentials.MQLClient")
def test_run_on_create_materialization_async_pending_status(
    mock_mql_client, mock_transform_credentials
):
    class MockMQLClient:
        def create_materialization(
            materialization_name: str,
            start_time: Optional[str] = None,
            end_time: Optional[str] = None,
            model_key_id: Optional[int] = None,
            output_table: Optional[str] = None,
            force: bool = False,
            warnings: List[str] = None,
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
                warnings=[],
            )

    mock_mql_client.return_value = MockMQLClient

    mock_transform_credentials.return_value = MockTransformCredentials
    mock_transform_credentials.get_client.return_value = mock_mql_client

    @flow(name="test_flow_10")
    def test_flow():
        return create_materialization(
            credentials=TransformCredentials(
                api_key=SecretStr("foo"), mql_server_url="foo"
            ),
            materialization_name="mt_name",
            wait_for_creation=False,
        )

    response = test_flow()

    assert response.is_complete is False
    assert response.is_successful is False
    assert response.is_failed is False


@mock.patch("prefect_transform.credentials.TransformCredentials")
@mock.patch("prefect_transform.credentials.MQLClient")
def test_run_on_create_materialization_async_running_status(
    mock_mql_client, mock_transform_credentials
):
    class MockMQLClient:
        def create_materialization(
            materialization_name: str,
            start_time: Optional[str] = None,
            end_time: Optional[str] = None,
            model_key_id: Optional[int] = None,
            output_table: Optional[str] = None,
            force: bool = False,
            warnings: List[str] = None,
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
                warnings=[],
            )

    mock_mql_client.return_value = MockMQLClient

    mock_transform_credentials.return_value = MockTransformCredentials
    mock_transform_credentials.get_client.return_value = mock_mql_client

    @flow(name="test_flow_11")
    def test_flow():
        return create_materialization(
            credentials=TransformCredentials(
                api_key=SecretStr("foo"), mql_server_url="foo"
            ),
            materialization_name="mt_name",
            wait_for_creation=False,
        )

    response = test_flow()

    assert response.is_complete is False
    assert response.is_successful is False
    assert response.is_failed is False


@mock.patch("prefect_transform.credentials.TransformCredentials")
@mock.patch("prefect_transform.credentials.MQLClient")
def test_run_on_create_materialization_sync(
    mock_mql_client, mock_transform_credentials
):
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

    mock_transform_credentials.return_value = MockTransformCredentials
    mock_transform_credentials.get_client.return_value = mock_mql_client

    @flow(name="test_flow_12")
    def test_flow():
        return create_materialization(
            credentials=TransformCredentials(
                api_key=SecretStr("foo"), mql_server_url="foo"
            ),
            materialization_name="mt_name",
        )

    response = test_flow()

    assert response.fully_qualified_name == "schema.table"
