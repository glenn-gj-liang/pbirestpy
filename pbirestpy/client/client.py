from typing import Annotated

from pandas import DataFrame
from .session import ApiSession
from ..auth import ServicePrincipalAuthenticator, StaticAuthenticator

AUTHENTICATOR = Annotated[
    ServicePrincipalAuthenticator | StaticAuthenticator,
    "An instance of an Authenticator to handle authentication.",
]


class PowerBIClient:
    """
    A client for interacting with the Power BI REST API.
    """

    def __init__(self, authenticator: AUTHENTICATOR):
        """
        Initializes the PowerBIClient with an authenticator.

        Args:
            authenticator (Authenticator): An instance of an Authenticator to handle authentication.
        """
        self.authenticator = authenticator
        self.session = ApiSession(self)

    @classmethod
    def from_service_principal(
        cls,
        tenant_id: str,
        client_id: str,
        client_secret: str,
    ) -> "PowerBIClient":
        """
        Creates a PowerBIClient instance using service principal authentication.

        Args:
            client_id (str): The client ID of the service principal.
            client_secret (str): The client secret of the service principal.
            tenant_id (str): The tenant ID.

        Returns:
            PowerBIClient: An instance of PowerBIClient.
        """
        authenticator = ServicePrincipalAuthenticator(
            client_id=client_id,
            client_secret=client_secret,
            tenant_id=tenant_id,
        )
        return cls(authenticator)

    @classmethod
    def from_token_literal(
        cls,
        access_token: str,
    ) -> "PowerBIClient":
        """
        Creates a PowerBIClient instance using a static token.
        Args:
            token (str): The static token to use for authentication.
        Returns:
            PowerBIClient: An instance of PowerBIClient.
        """
        authenticator = StaticAuthenticator(access_token=access_token)
        return cls(authenticator)

    async def __aenter__(self):
        """
        Asynchronous context manager entry method.
        Initializes the session.
        """
        await self.session._ensure_session()
        return self.session

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Asynchronous context manager exit method.
        Closes the session.
        """
        await self.session.close()

    @staticmethod
    def to_df(elements):
        return DataFrame([element.to_dict() for element in elements])

    @staticmethod
    def to_spark_df(elements):
        """
        Converts a list of elements to a Spark DataFrame.

        Args:
            elements (list): A list of elements to convert.

        Returns:
            DataFrame: A Spark DataFrame containing the elements.
        """
        from databricks.sdk.runtime import spark  # type: ignore

        return spark.createDataFrame(PowerBIClient.to_df(elements))
