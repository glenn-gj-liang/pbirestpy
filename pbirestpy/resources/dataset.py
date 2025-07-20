from typing import Optional, List, Dict
from .base import BaseResource
from .dax import DaxExecutor

TOP_REFRESHES_COUNT = 50


class Dataset(BaseResource):
    """
    Represents a dataset resource in the Power BI REST API.

    This class provides methods to interact with datasets, such as retrieving,
    creating, updating, and deleting datasets.
    """

    __SLOTS__ = (
        "id",
        "name",
        "webUrl",
        "isRefreshable",
        "createdDate",
        "description",
        "configuredBy",
        "addRowsAPIEnabled",
        "isEffectiveIdentityRequired",
        "isEffectiveIdentityRolesRequired",
        "isOnPremGatewayRequired",
        "targetStorageMode",
        "createReportEmbedUrl",
        "qnaEmbedUrl",
        "upstreamDatasets",
        "users",
        "queryScaleOutSettings",
        "group.name",
        "group_id",
    )

    def __init__(
        self,
        webUrl: str,
        isRefreshable: bool,
        createdDate: str,
        description: Optional[str] = None,
        configuredBy: Optional[str] = None,
        addRowsAPIEnabled: bool = False,
        isEffectiveIdentityRequired: bool = False,
        isEffectiveIdentityRolesRequired: bool = False,
        isOnPremGatewayRequired: bool = False,
        targetStorageMode: Optional[str] = None,
        createReportEmbedUrl: Optional[str] = None,
        qnaEmbedUrl: Optional[str] = None,
        upstreamDatasets: Optional[List] = None,
        users: Optional[List] = None,
        queryScaleOutSettings: Optional[Dict] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.webUrl = webUrl
        self.isRefreshable = isRefreshable
        self.createdDate = createdDate
        self.description = description
        self.configuredBy = configuredBy
        self.addRowsAPIEnabled = addRowsAPIEnabled
        self.isEffectiveIdentityRequired = isEffectiveIdentityRequired
        self.isEffectiveIdentityRolesRequired = (
            isEffectiveIdentityRolesRequired
        )
        self.isOnPremGatewayRequired = isOnPremGatewayRequired
        self.targetStorageMode = targetStorageMode
        self.createReportEmbedUrl = createReportEmbedUrl
        self.qnaEmbedUrl = qnaEmbedUrl
        self.upstreamDatasets = upstreamDatasets or []
        self.users = users or []
        self.queryScaleOutSettings = queryScaleOutSettings or {}
        self.dax = DaxExecutor(self)

    @property
    def dax_query_url(self):
        return self.build_url(
            f"groups/{self.group_id}/datasets/{self.id}/executeQueries"
        )

    @property
    def list_refreshes_url(self):
        return self.build_url(
            f"groups/{self.group_id}/datasets/{self.id}/refreshes?top={TOP_REFRESHES_COUNT}"
        )

    @property
    def get_schedule_url(self):
        return self.build_url(
            f"groups/{self.group_id}/datasets/{self.id}/refreshSchedule"
        )

    @property
    def start_refresh_url(self):
        return self.build_url(
            f"groups/{self.group_id}/datasets/{self.id}/refreshes"
        )
