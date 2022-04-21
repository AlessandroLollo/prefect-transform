from prefect import flow

from prefect_transform.tasks import (
    goodbye_prefect_transform,
    hello_prefect_transform,
)


def test_hello_prefect_transform():
    @flow
    def test_flow():
        return hello_prefect_transform()

    flow_state = test_flow()
    task_state = flow_state.result()
    assert task_state.result() == "Hello, prefect-transform!"


def goodbye_hello_prefect_transform():
    @flow
    def test_flow():
        return goodbye_prefect_transform()

    flow_state = test_flow()
    task_state = flow_state.result()
    assert task_state.result() == "Goodbye, prefect-transform!"
