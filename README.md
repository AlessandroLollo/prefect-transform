# prefect-transform

## Welcome!

Collection of tasks to interact with Transform metrics catalog

## Getting Started

### Python setup

Requires an installation of Python 3.7+.

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://orion-docs.prefect.io/).

### Installation

Install `prefect-transform` with `pip`:

```bash
pip install prefect-transform
```

### Write and run a flow

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

## Resources

If you encounter any bugs while using `prefect-transform`, feel free to open an issue in the [prefect-transform](https://github.com/AlessandroLollo/prefect-transform) repository.

If you have any questions or issues while using `prefect-transform`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

## Development

If you'd like to install a version of `prefect-transform` for development, clone the repository and perform an editable install with `pip`:

```bash
git clone https://github.com/AlessandroLollo/prefect-transform.git

cd prefect-transform/

pip install -e ".[dev]"

# Install linting pre-commit hooks
pre-commit install
```
