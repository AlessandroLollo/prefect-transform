"""Collection of tasks to interact with Transform metrics catalog"""
import os
from typing import Optional, Union

from prefect import task
from transform import MQLClient
from transform.exceptions import AuthException, QueryRuntimeException, URLException
from transform.models import MqlMaterializeResp, MqlQueryStatusResp

from prefect_transform.exceptions import (
    TransformAuthException,
    TransformConfigurationException,
    TransformRuntimeException,
)


@task
def create_materialization(
    api_key: Optional[str] = None,
    api_key_env_var: Optional[str] = None,
    mql_server_url: Optional[str] = None,
    mql_server_url_env_var: Optional[str] = None,
    materialization_name: str = None,
    model_key_id: Optional[int] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    output_table: Optional[str] = None,
    force: Optional[bool] = False,
    wait_for_creation: Optional[bool] = True,
) -> Union[MqlMaterializeResp, MqlQueryStatusResp]:
    """
    Task to create a materialization against a Transform metrics layer
    deployment.
    Please refer to [Transform official documentation](https://docs.transform.co/)
    for more information.
    This task uses [Transform official MQL Client](https://pypi.org/project/transform/)
    under the hood.

    Args:
        api_key: Transform API Key to be used to
            connect to Transform MQL Server.
        api_key_env_var: The name of the environment variable
            that contains the API Key to be used to connect to Transform MQL Server.
        mql_server_url: The URL of the Transform MQL Server
            from which to create the materialization.
        mql_server_url_env_var: The name of the environment variable
            that contains the URL of the Transform MQL Server from which to
            create the materialization.
        materialization_name: The name of the Transform
            materialization to create.
        model_key_id: The unique identifier of the Transform model
            against which the transformation will be created.
        start_time: The UTC start time of the materialization.
        end_time: The UTC end time of the materialization.
        output_table: The name of the database table, in the form of
            `schema_name.table_name`, where the materialization will be created.
        force: Whether to force the materialization creation
            or not. Defaults to `False`.
        wait_for_creation: Whether to wait for the materialization
            creation or not. Defaults to `True`.

    Raises:
        `TransformConfigurationException` if both `api_key`
            and `api_key_env_var` are missing.
        `TransformConfigurationException` if both `mql_server_url`
            and `mql_server_url_env_var` are missing.
        `TransformConfigurationException` if `materialization_name` is missing.
        `TransformAuthException` if the connection with the Transform
            server cannot be established.
        `TransformRuntimeException` if the materialization creation process fails.

    Returns:
        An `MqlQueryStatusResp` object if `run_async` is `True`.
        An `MqlMaterializeResp` object if `run_async` is `False`.
    """
    # Raise error if both api_key and api_key_env_var are missing
    if not (api_key or api_key_env_var):
        msg = "Both `api_key` and `api_key_env_var` are missing."
        raise TransformConfigurationException(msg)

    # Raise error if api_key is missing and env var is not found
    if not api_key and api_key_env_var not in os.environ.keys():
        msg = "`api_key` is missing and `api_key_env_var` not found in env vars."
        raise TransformConfigurationException(msg)

    # Raise error if both mql_server_url and mql_server_url_env_var are missing
    if not (mql_server_url or mql_server_url_env_var):
        msg = "Both `mql_server_url` and `mql_server_url_env_var` are missing."
        raise TransformConfigurationException(msg)

    # Raise error if mql_server_url is missing and env var is not found
    if not mql_server_url and mql_server_url_env_var not in os.environ.keys():
        msg = """
        `mql_server_url` is missing and `mql_server_url_env_var` not found in env vars.
        """
        raise TransformConfigurationException(msg)

    if not materialization_name:
        msg = "`materialization_name` is missing."
        raise TransformConfigurationException(msg)

    mql_api_key = api_key or os.environ[api_key_env_var]
    mql_url = mql_server_url or os.environ[mql_server_url_env_var]
    use_async = not wait_for_creation

    try:
        mql_client = MQLClient(
            api_key=mql_api_key, mql_server_url=mql_url, use_async=use_async
        )
    except (AuthException, URLException) as e:
        msg = f"Cannot connect to Transform server! Error is: {e.msg}"
        raise TransformAuthException(msg)

    response = None
    if use_async:
        response = mql_client.create_materialization(
            materialization_name=materialization_name,
            start_time=start_time,
            end_time=end_time,
            model_key_id=model_key_id,
            output_table=output_table,
            force=force,
        )
        if response.is_failed:
            msg = f"""
            Transform materialization async creation failed! Error is: {response.error}
            """
            raise TransformRuntimeException(msg)
    else:
        try:
            response = mql_client.materialize(
                materialization_name=materialization_name,
                start_time=start_time,
                end_time=end_time,
                model_key_id=model_key_id,
                output_table=output_table,
                force=force,
            )
        except QueryRuntimeException as e:
            msg = f"Transform materialization sync creation failed! Error is: {e.msg}"
            raise TransformRuntimeException(msg)

    return response
