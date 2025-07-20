from typing import Optional, List, Dict
from .base import BaseRefreshable


class Dataflow(BaseRefreshable):
    """
    Represents a dataflow resource in the Power BI REST API.

    This class provides methods to interact with dataflows, such as retrieving,
    creating, updating, and deleting dataflows.
    """

    __SLOTS__ = (
        "id",
        "name",
        "configuredBy",
        "users",
        "description",
        "generation",
        "group.name",
        "group_id",
    )

    def __init__(
        self,
        objectId: Optional[str] = "",
        configuredBy: Optional[str] = None,
        users: Optional[List] = None,
        description: Optional[str] = "",
        generation: Optional[int] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.objectId = objectId
        self.configuredBy = configuredBy
        self.users = users
        self.description = description
        self.generation = generation
        self.id = self.objectId

    @property
    def list_refreshes_url(self) -> str:
        """
        Returns the URL to list refreshes for the dataflow.

        Returns:
            str: The URL to list refreshes for the dataflow.
        """
        return self.build_url(
            f"groups/{self.group_id}/dataflows/{self.id}/transactions"
        )

    @property
    def start_refresh_url(self):
        return self.build_url(
            f"groups/{self.group_id}/dataflows/{self.id}/refreshes?procesType=default"
        )
