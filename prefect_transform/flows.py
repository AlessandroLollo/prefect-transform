"""This is an example flows module"""
from prefect import flow

from prefect_transform.tasks import (
    goodbye_prefect_transform,
    hello_prefect_transform,
)


@flow
def hello_and_goodbye():
    """
    Sample flow that says hello and goodbye!
    """
    print(hello_prefect_transform)
    print(goodbye_prefect_transform)
