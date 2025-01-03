from dataclasses import asdict, dataclass
from datetime import datetime, UTC as utc_tz
from hashlib import sha256
from typing import Dict, List, Optional, Union
from uuid import uuid4

from da_vinci.core.orm import (
    TableClient,
    TableObject,
    TableObjectAttribute,
    TableObjectAttributeType,
    TableScanDefinition,
)


class LakeRequestChainRunningRequest(TableObject):
    table_name = "lake_request_chain_running_requests"

    description = "Tracks all of the lake requests that a chain is waiting on."

    partition_key_attribute = TableObjectAttribute(
        name="lake_request_id",
        attribute_type=TableObjectAttributeType.STRING,
        description="The unique identifier for the request",
    )

    attributes = [
        TableObjectAttribute(
            name="chain_request_id",
            attribute_type=TableObjectAttributeType.STRING,
            description="The unique identifier for the request chain.",
        ),

        TableObjectAttribute(
            name="chain_request_name",
            attribute_type=TableObjectAttributeType.STRING,
            description="The unique name for the request.",
        ),

        TableObjectAttribute(
            name="created_on",
            attribute_type=TableObjectAttributeType.DATETIME,
            description="The date and time the request was created.",
            default=lambda: datetime.now(utc_tz),
        ),
    ]

    def __init__(self, chain_request_id: str, chain_request_name: str, lake_request_id: str,
                 created_on: Optional[datetime] = None):
        """
        Initialize a LakeRequestChainRunningRequest object.

        Keyword Arguments:
        chain_request_id -- The unique identifier for the request chain.
        chain_request_name -- The unique name for the request.
        created_on -- The date and time the request was created.
        lake_request_id -- The unique identifier for the request
        """
        super().__init__(
            chain_request_id=chain_request_id,
            chain_request_name=chain_request_name,
            created_on=created_on,
            lake_request_id=lake_request_id,
        )


class LakeRequestChainRunningRequestsScanDefinition(TableScanDefinition):
    def __init__(self):
        super().__init__(table_object_class=LakeRequestChainRunningRequest)


class LakeRequestChainRunningRequestsClient(TableClient):
    def __init__(self, app_name: Optional[str] = None, deployment_id: Optional[str] = None):
        super().__init__(
            app_name=app_name,
            default_object_class=LakeRequestChainRunningRequest,
            deployment_id=deployment_id,
        )

    def get(self, pending_request_id: str) -> Optional[LakeRequestChainRunningRequest]:
        """
        Get a LakeRequestChainPending object by its lake_request_id.

        Keyword Arguments:
        lake_request_id -- The lake_request_id to get.
        """
        return self.get_object(pending_request_id)

    def delete(self, running_request: LakeRequestChainRunningRequest) -> None:
        """
        Delete a running request object.

        Keyword Arguments:
        lake_request_id -- The lake_request_id to delete.
        """
        self.delete_object(running_request)

    def put(self, running_request: LakeRequestChainRunningRequest) -> None:
        """
        Put a LakeRequestChainPending object.

        Keyword Arguments:
        pending_request -- The LakeRequestChainPending object to put.
        """
        self.put_object(running_request)