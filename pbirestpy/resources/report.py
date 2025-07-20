from typing import Optional, List, Dict
from .base import BaseResource


class Report(BaseResource):
    __SLOTS__ = (
        "id",
        "name",
        "reportType",
        "webUrl",
        "embedUrl",
        "isFromPbix",
        "isOwnedByMe",
        "datasetId",
        "datasetWorkspaceId",
        "users",
        "subscriptions",
        "reportFlags",
        "description",
        "group.name",
        "group_id",
    )

    def __init__(
        self,
        reportType: str,
        webUrl: str,
        embedUrl: Optional[str],
        isFromPbix: Optional[bool] = False,
        isOwnedByMe: Optional[bool] = False,
        datasetId: Optional[str] = None,
        datasetWorkspaceId: Optional[str] = None,
        users: Optional[List[Dict]] = None,
        subscriptions: Optional[List[Dict]] = None,
        reportFlags: Optional[List[str]] = None,
        description: Optional[str] = None,
        group_id: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.reportType = reportType
        self.webUrl = webUrl
        self.embedUrl = embedUrl
        self.isFromPbix = isFromPbix
        self.isOwnedByMe = isOwnedByMe
        self.datasetId = datasetId
        self.datasetWorkspaceId = datasetWorkspaceId
        self.users = users or []
        self.subscriptions = subscriptions or []
        self.reportFlags = reportFlags or []
        self.description = description

    @property
    def list_pages_url(self) -> str:
        """
        Returns the URL to list all pages of the report.
        """
        return self.build_url(
            f"groups/{self.group_id}/reports/{self.id}/pages"
        )
