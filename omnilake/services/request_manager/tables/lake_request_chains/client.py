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

from da_vinci.core.immutable_object import ObjectBody


class LakeRequestChain(TableObject):
    table_name = "lake_request_chains"

    description = "Tracks all of the lake request chains in the system."

    partition_key_attribute = TableObjectAttribute(
        name="chain_request_id",
        attribute_type=TableObjectAttributeType.STRING,
        description="The unique identifier for the request chain.",
        default=lambda: str(uuid4()),
    )

    attributes = [
        TableObjectAttribute(
            name="chain",
            attribute_type=TableObjectAttributeType.JSON_STRING,
            description="The raw representation of a request chain.",
        ),

        TableObjectAttribute(
            name="chain_execution_status",
            attribute_type=TableObjectAttributeType.STRING,
            description="The status of the request chain.",
            default="PENDING",
        ),

        TableObjectAttribute(
            name="created_on",
            attribute_type=TableObjectAttributeType.DATETIME,
            description="The date and time the request chain was created.",
            default=lambda: datetime.now(utc_tz),
        ),

        TableObjectAttribute(
            name="ended",
            attribute_type=TableObjectAttributeType.DATETIME,
            description="The date and time the request chain ended.",
            optional=True,
            default=None,
        ),

        TableObjectAttribute(
            name="executed_requests",
            attribute_type=TableObjectAttributeType.JSON,
            description="The dictionary of request names/ids that have already been executed in the request chain.",
            optional=True,
            default={},
        ),

        TableObjectAttribute(
            name="job_id",
            attribute_type=TableObjectAttributeType.STRING,
            description="The job ID of the request chain.",
        ),

        TableObjectAttribute(
            name="job_type",
            attribute_type=TableObjectAttributeType.STRING,
            description="The type of job that the request chain is associated with.",
        ),

        TableObjectAttribute(
            name="num_remaining_running_requests",
            attribute_type=TableObjectAttributeType.NUMBER,
            description="The number of requests that are currently running in the request chain.",
            default=0,
            optional=True,
        ),

        TableObjectAttribute(
            name="started",
            attribute_type=TableObjectAttributeType.DATETIME,
            description="The date and time the request chain started.",
            optional=True,
            default=lambda: datetime.now(utc_tz),
        ),

        TableObjectAttribute(
            name="status",
            attribute_type=TableObjectAttributeType.STRING,
            description="The status of the request chain.",
            default="PENDING",
        )
    ]

    def __init__(self, chain: Dict, chain_request_id: str, job_id: str, job_type: str,
                 chain_execution_status: Optional[str] = None, created_on: Optional[datetime] = None, ended: Optional[datetime] = None,
                 executed_requests: Optional[List[ObjectBody]] = None, num_remaining_running_requests: Optional[int] = 0,
                 running_requests: Optional[List[ObjectBody]] = None, status: Optional[str] = None, started: Optional[datetime] = None,):
        """
        Initializes a new LakeRequestChain object.

        Keyword Arguments:
        chain -- The full chain of requests to be executed.
        chain_execution_status -- The status of the request chain.
        chain_request_id -- The ID of the request chain.
        created_on -- The date and time the request chain was created.
        ended -- The date and time the request chain ended.
        executed_requests -- The list of request names that have already been executed in the
            request chain.
        job_id -- The job ID of the request chain.
        job_type -- The type of job that the request chain is associated with.
        num_remaining_running_requests -- The number of requests that are currently running in the request chain.
        running_requests -- The list of request names that are currently running.
        started -- The date and time the request chain started.
        status -- The status of the request chain.
        """
        super().__init__(
            chain=chain,
            chain_request_id=chain_request_id,
            chain_execution_status=chain_execution_status,
            created_on=created_on,
            ended=ended,
            executed_requests=executed_requests,
            job_id=job_id,
            job_type=job_type,
            num_remaining_running_requests=num_remaining_running_requests,
            running_requests=running_requests,
            started=started,
            status=status,
        )


class LakeRequestChainsScanDefinition(TableScanDefinition):
    def __init__(self):
        super().__init__(table_object_class=LakeRequestChain)


class LakeRequestChainsClient(TableClient):
    def __init__(self, app_name: Optional[str] = None, deployment_id: Optional[str] = None):
        """
        Initialize the lake requests chains Client

        Keyword Arguments:
            app_name -- The name of the app.
            deployment_id -- The deployment ID.
        """
        super().__init__(
            app_name=app_name,
            deployment_id=deployment_id,
            default_object_class=LakeRequestChain,
        )

    def record_lake_request_results(self, lake_request_id: str, reference_name: str, request_chain_id: str) -> int:
        """
        Add lookup results to the lake request

        Keyword arguments:
        decrement_process -- Whether or not to decrement the remaining processes (default: {True})
        lake_request_id -- The request ID of the compaction job context
        reference_name -- The name of the resource to add
        """
        update_expression = "SET ExecutedRequests.#key = :value SET RemainingRunningRequests = if_not_exists(RemainingRunningRequests, :start) - :decrement"

        expression_attr_names = {
            '#key': reference_name,
        }

        expression_attribute_values = {
            ':decrement': {'N': "1"},
            ':start': {'N': "0"},
            ':value': lake_request_id,
        }

        response = self.client.update_item(
            TableName=self.table_endpoint_name,
            Key={
                'RequestChainId': {'S': request_chain_id},
            },
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_attr_names,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues='UPDATED_NEW',
        )

        updated_remaining_lookups = int(response['Attributes']['RemainingRunningRequests']['N'])
    
        return updated_remaining_lookups

    def delete(self, lake_request_chain: LakeRequestChain) -> None:
        """
        Delete an lake request chain object from the table

        Keyword Arguments:
            lake_request_chain -- The lake request chain object.
        """
        return self.delete_object(lake_request_chain)

    def get(self, lake_chain_request_id: str, consistent_read: Optional[bool] = False) -> Union[LakeRequestChain, None]:
        """
        Get an lake request chain object from the table

        Keyword Arguments:
            lake_chain_request_id -- The lake chain request id.
            consistent_read -- Whether or not to use consistent read.
        """
        return self.get_object(partition_key_value=lake_chain_request_id, consistent_read=consistent_read)

    def put(self, lake_request_chain: LakeRequestChain) -> None:
        """
        Put an lake request chain object into the table

        Keyword Arguments:
            lake_request_chain -- The lake request chain object.
        """
        return self.put_object(lake_request_chain)