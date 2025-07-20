from typing import Optional
from .base import BaseResource


class Group(BaseResource):
    """
    Represents a Power BI Group (Workspace)
    """

    __SLOTS__ = (
        "id",
        "name",
        "isReadOnly",
        "type",
        "isOnDedicatedCapacity",
        "capacityId",
        "defaultDatasetStorageFormat",
    )

    LIST_GROUPS_URL = BaseResource.build_url("groups")

    def __init__(
        self,
        isReadOnly: bool,
        type: str,
        isOnDedicatedCapacity: Optional[bool] = False,
        capacityId: Optional[str] = "",
        defaultDatasetStorageFormat: Optional[str] = "",
        *args,
        **kwargs,
    ):
        """
        Initializes the Group with its properties.

        Args:
            isReadOnly: Indicates if the group is read-only.
            type: The type of the group.
            isOnDedicatedCapacity: Indicates if the group is on dedicated capacity.
            capacityId: The ID of the capacity if applicable.
            defaultDatasetStorageFormat: The default storage format for datasets in the group.
        """
        super().__init__(*args, **kwargs)
        self.isReadOnly = isReadOnly
        self.type = type
        self.isOnDedicatedCapacity = isOnDedicatedCapacity
        self.capacityId = capacityId
        self.defaultDatasetStorageFormat = defaultDatasetStorageFormat
        self.group_id = self.id

    @property
    def list_datasets_url(self):
        """
        Returns the URL to list datasets in the group.

        Returns:
            str: The URL to list datasets in the group.
        """
        return BaseResource.build_url(f"groups/{self.id}/datasets")

    @property
    def list_reports_url(self):
        """
        Returns the URL to list reports in the group.

        Returns:
            str: The URL to list reports in the group.
        """
        return BaseResource.build_url(f"groups/{self.id}/reports")

    @property
    def list_dataflows_url(self):
        """
        Returns the URL to list dataflows in the group.

        Returns:
            str: The URL to list dataflows in the group.
        """
        return BaseResource.build_url(f"groups/{self.id}/dataflows")
