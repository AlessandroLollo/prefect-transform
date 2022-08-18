"""Collection of tasks to interact with Transform metrics catalog"""
from typing import Optional, Union

from prefect import task
from transform.exceptions import QueryRuntimeException
from transform.models import MqlMaterializeResp, MqlQueryStatusResp

from prefect_transform.credentials import TransformCredentials
from prefect_transform.exceptions import TransformRuntimeException


@task
def create_materialization(
    credentials: TransformCredentials,
    materialization_name: str,
    model_key_id: Optional[int] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    output_table: Optional[str] = None,
    force: bool = False,
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
        credentials: `TransformCredentials` object used to obtain a client to
            interact with Transform.
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
        `TransformConfigurationException` if `materialization_name` is missing.
        `TransformAuthException` if the connection with the Transform
            server cannot be established.
        `TransformRuntimeException` if the materialization creation process fails.

    Returns:
        An `MqlQueryStatusResp` object if `run_async` is `True`.
        An `MqlMaterializeResp` object if `run_async` is `False`.

    Example:
    ```python
    from prefect import flow
    from prefect_transform.tasks import (
        create_materialization
    )


    @flow
    def trigger_materialization_creation():
        create_materialization(
            api_key="<your Transform API key>",
            mql_server_url="<your MQL Serverl URL>",
            materialization_name="<name of the materialization>",
            wait_for_creation=False
        )

    trigger_materialization_creation()
    ```
    """
    use_async = not wait_for_creation
    mql_client = credentials.get_client()

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
