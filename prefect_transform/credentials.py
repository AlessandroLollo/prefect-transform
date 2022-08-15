"""Transform credentials block"""
import os
from typing import Optional

from prefect.blocks.core import Block
from pydantic import SecretStr, root_validator
from transform import MQLClient
from transform.exceptions import AuthException, URLException

from prefect_transform.exceptions import (
    TransformAuthException,
    TransformConfigurationException,
)


class TransformCredentials(Block):
    """
    Block used to manage authentication with Transform.
    """

    api_key: Optional[SecretStr] = None
    api_key_env_var: Optional[str] = None
    mql_server_url: Optional[str] = None
    mql_server_url_env_var: Optional[str] = None

    @root_validator(pre=True)
    def check_credentials(cls, values):
        """
        Ensure that and API Key and MQL Server URL are provided directly
        or through environment variables.
        """

        _api_key = values.get("api_key")
        _api_key_env_var = values.get("api_key_env_var")

        if not (_api_key or _api_key_env_var):
            msg = "Both `api_key` and `api_key_env_var` are missing."
            raise TransformConfigurationException(msg)

        if not (_api_key or _api_key_env_var in os.environ.keys()):
            msg = "`api_key` is missing and `api_key_env_var` not found in env vars."
            raise TransformConfigurationException(msg)

        _mql_server_url = values.get("mql_server_url")
        _mql_server_url_env_var = values.get("mql_server_url_env_var")

        if not (_mql_server_url or _mql_server_url_env_var):
            msg = "Both `mql_server_url` and `mql_server_url_env_var` are missing."
            raise TransformConfigurationException(msg)

        if not (_mql_server_url or _mql_server_url_env_var in os.environ.keys()):
            msg = """
            `mql_server_url` is missing and `mql_server_url_env_var`
            not found in env vars.
            """
            raise TransformConfigurationException(msg)

        return values

    def get_client(self) -> MQLClient:
        """
        Return an MQLClient that can be used to interact with
        Transform server.
        """

        _api_key = self.api_key.get_secret_value() or os.environ.get(
            self.api_key_env_var
        )
        _mql_server_url = self.mql_server_url or os.environ.get(
            self.mql_server_url_env_var
        )

        try:
            return MQLClient(api_key=_api_key, mql_server_url=_mql_server_url)
        except (AuthException, URLException) as e:
            msg = f"Cannot connect to Transform server! Error is: {e}"
            raise TransformAuthException(msg)
