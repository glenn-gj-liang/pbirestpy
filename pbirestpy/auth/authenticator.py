import msal
from datetime import timedelta
from .token import Token, StaticBearerToken


class ServicePrincipalAuthenticator:
    """
    A class to handle service principal authentication using a static bearer token.
    This is useful for scenarios where a static token is provided for authentication.
    """

    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        expires_in: int = 1200,
    ):
        """
        Initializes the ServicePrincipalAuthenticator with the necessary credentials.
        Args:
            tenant_id (str): The Azure AD tenant ID.
            client_id (str): The client ID of the service principal.
            client_secret (str): The client secret of the service principal.
            expires_in (int): The token expiration time in seconds. Defaults to 1200 seconds (20 minutes).
        """
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.authority = f"https://login.microsoftonline.com/{tenant_id}"
        self.app = msal.ConfidentialClientApplication(
            client_id=client_id,
            client_credential=client_secret,
            authority=self.authority,
        )
        self.scope = ["https://analysis.windows.net/powerbi/api/.default"]
        self.expires_in = timedelta(seconds=expires_in)
        self.__token: Token | None = None

    def aquire_token(self):
        """
        Acquires a token from Azure AD using the client credentials flow.
        """
        result: dict = self.app.acquire_token_for_client(scopes=self.scope)  # type: ignore
        if "access_token" in result:
            self.__token = Token(
                access_token=result["access_token"], expires_in=self.expires_in
            )
        else:
            raise Exception(
                f"Failed to acquire token: {result.get('error_description', 'Unknown error')}"
            )

    @property
    def token(self) -> str:
        """
        Returns the current token, acquiring a new one if the current token is expired.
        Returns:
            str: The current access token.
        """
        if self.__token is None or self.__token.is_expired:
            self.aquire_token()
        return str(self.__token)


class StaticAuthenticator:
    """
    A class to handle static authentication using a static bearer token.
    This is useful for scenarios where a static token is provided for authentication.
    """

    def __init__(self, access_token: str):
        """
        Initializes the StaticAuthenticator with a static bearer token.

        Args:
            access_token (str): The static bearer token.
        """
        self.token = str(StaticBearerToken(access_token))
