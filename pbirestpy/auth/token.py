from datetime import datetime, timedelta
from dataclasses import dataclass, field


@dataclass
class Token:
    """A class representing an authentication token."""

    access_token: str
    expires_in: timedelta = timedelta(minutes=10)
    issued_at: datetime = field(default_factory=datetime.now)
    token_type: str = "Bearer"

    @property
    def expires_at(self) -> datetime:
        """
        Returns the expiration time of the token.

        Returns:
            datetime: The time when the token will expire.
        """
        return self.issued_at + self.expires_in

    @property
    def is_expired(self) -> bool:
        """
        Checks if the token is expired.

        Returns:
            bool: True if the token is expired, False otherwise.
        """
        return datetime.now() >= self.expires_at

    def __str__(self) -> str:
        """
        Returns a string representation of the token.

        Returns:
            str: The access token.
        """
        return f"{self.token_type} {self.access_token}"


class StaticBearerToken:
    def __init__(self, access_token: str):
        """
        Initializes the StaticBearerToken with a token.

        Args:
            token (str): The static bearer token.
        """
        self.access_token = access_token

    def __str__(self) -> str:
        """
        Returns a string representation of the static bearer token.

        Returns:
            str: The static bearer token.
        """
        return f"Bearer {self.access_token}"
