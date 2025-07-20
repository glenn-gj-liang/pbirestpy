from typing import Optional, List, Dict
from .base import BaseResource


class Dataflow(BaseResource):
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
        configuredBy: Optional[str] = None,
        users: Optional[List] = None,
        description: Optional[str] = "",
        generation: Optional[int] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.configuredBy = configuredBy
        self.users = users
        self.description = description
        self.generation = generation

    @property
    def list_refreshes_url(self) -> str:
        """
        Returns the URL to list refreshes for the dataflow.

        Returns:
            str: The URL to list refreshes for the dataflow.
        """
        return BaseResource.build_url(
            f"groups/{self.group_id}/dataflows/{self.id}/refreshes?procesType=default"
        )
