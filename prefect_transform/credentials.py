"""Transform credentials block"""
from prefect.blocks.core import Block
from pydantic import Field, SecretStr, root_validator
from transform import MQLClient
from transform.exceptions import AuthException, URLException

from prefect_transform.exceptions import TransformAuthException


class TransformCredentials(Block):
    """
    Block used to manage authentication with Transform.

    Args:
        api_key (SecretStr): The API key to use to connect to Transform.
        mql_server_url (str): The URL of the Transform MQL server.

    Example:
        Load stored Transform credentials
        ```python
        from prefect_transform.credentials import TransformCredentials
        transform_credentials_block = TransformCredentials.load("BLOCK_NAME")
        ```
    """  # noqa E501

    _block_type_name = "Transform Credentials"

    api_key: SecretStr = Field(..., description="Transform API key")
    mql_server_url: str = Field(..., description="Transform MQL Server URL")

    @root_validator(pre=True)
    def check_credentials(cls, values):
        """
        Ensure the API key and the MQL Server URL are actually
        passed.
        """
        # Given that both values are required, is this really needed?
        return values

    def get_client(self) -> MQLClient:
        """
        Return an MQLClient that can be used to interact with
        Transform server.

        Returns:
            An `MQLClient` that can be used to interact with Transform server.
        """

        _api_key = self.api_key.get_secret_value()

        try:
            return MQLClient(api_key=_api_key, mql_server_url=self.mql_server_url)
        except (AuthException, URLException) as e:
            msg = f"Cannot connect to Transform server! Error is: {e}"
            raise TransformAuthException(msg)