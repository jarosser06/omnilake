"""
Handles coordinating LakeRequest Chains

Example Chain:
```
{
    "requests": [
        {
            "name": "request_1",
            "lookup_instructions": [
                {
                    "archive_id": "omnilake",
                    "max_entries": 20,
                    "query_string": "What is OmniLake?",
                    "prioritize_tags": None,
                    "request_type": "VECTOR"
                }
            ],
            "processing_instructions": {
                "goal": "Describe what OmniLake is and how it works",
                "include_source_metadata": False,
                "model_id": None,
                "processor_type": "SUMMARIZATION",
                "prompt": None
            },
            "response_config": {
                "destination_archive_id": None,
                "goal": "Anwer the following question: What is OmniLake?",
                "model_id": None,
                "response_type": "SIMPLE"
            }
        },
        {
            "name": "request_1",
            "lookup_instructions": [
                {
                    "entry_id": "REF:response_1",
                    "request_type": "DIRECT_ENTRY"
                }
            ],
            "processing_instructions": {
                "goal": "How would OmniLake help build scalable AI applications?",
                "include_source_metadata": False,
                "model_id": None,
                "processor_type": "SUMMARIZATION",
                "prompt": None
            },
            "response_config": {
                "destination_archive_id": None,
                "goal": "How would OmniLake help build scalable AI applications?",
                "model_id": None,
                "response_type": "SIMPLE"
            }
        }
    ]
}
```
"""
import logging

from datetime import datetime, UTC as utc_tz
from typing import List, Optional

from da_vinci.core.immutable_object import (
    ObjectBody,
    ObjectBodySchema,
    SchemaAttribute,
    SchemaAttributeType,
)
from da_vinci.core.logging import Logger

from da_vinci.event_bus.client import EventPublisher, fn_event_response
from da_vinci.event_bus.event import Event as EventBusEvent

from da_vinci.exception_trap.client import ExceptionReporter

from omnilake.internal_lib.event_definitions import (
    LakeRequestEventBodySchema,
    LakeChainRequestEventBodySchema,
    LakeCompletionEventBodySchema,
)

from omnilake.services.request_manager.runtime.primitive_lookup import (
    DirectEntryLookupSchema,
)

from omnilake.tables.jobs.client import JobsClient, JobStatus
from omnilake.tables.lake_requests.client import (
    LakeRequest,
    LakeRequestsClient,
    LakeRequestStage,
    LakeRequestStatus,
)

# Local imports
from omnilake.services.request_manager.tables.lake_request_chains.client import (
    LakeRequestChain,
    LakeRequestChainsClient,
)

from omnilake.services.request_manager.tables.lake_request_chain_running_requests.client import (
    LakeRequestChainRunningRequest,
    LakeRequestChainRunningRequestsClient,
)


class LakeChainRequestSchema(ObjectBodySchema):
    """
    Represents a request in a LakeRequest Chain
    """
    attributes = [
        SchemaAttribute(name="lookup_instructions", type=SchemaAttributeType.OBJECT_LIST),
        SchemaAttribute(name="name", type=SchemaAttributeType.STRING),
        SchemaAttribute(name="processing_instructions", type=SchemaAttributeType.OBJECT),
        SchemaAttribute(name="response_config", type=SchemaAttributeType.OBJECT),
    ]


class LakeChainSchema(ObjectBodySchema):
    """
    Represents a LakeRequest Chain
    """
    attributes = [
        SchemaAttribute(name="requests", object_schema=LakeChainRequestSchema, type=SchemaAttributeType.OBJECT_LIST),
    ]


class LakeChainExecutedRequestSchema(ObjectBodySchema):
    """
    Represents a request that has been executed in a LakeRequest Chain
    """
    attributes = [
        SchemaAttribute(name="name", type=SchemaAttributeType.STRING),
        SchemaAttribute(name="lake_request_id", type=SchemaAttributeType.STRING),
    ]


class ChainCoordinator:
    """
    Handles coordinating LakeRequest Chains
    """
    REFERENCE_PREFIX = "REF:"

    def __init__(self, chain: ObjectBody, chain_request_id: Optional[str] = None,
                 executed_requests: Optional[List[ObjectBody]] = None):
        """
        Initializes a new ChainCoordinator object.

        Keyword Arguments:
        chain -- The full chain of requests to be executed
        chain_request_id -- The ID of the request chain
        executed_requests -- The list of requests that have already been
        """
        self.chain = chain.map_to(new_schema=LakeChainSchema)

        self.executed_requests = {}

        self.chain_request_id = chain_request_id

        executed_reqs = [req.map_to(new_schema=LakeChainExecutedRequestSchema) for req in executed_requests] or []

        for exec_req in executed_reqs:
            self.executed_requests[exec_req["name"]] = exec_req["lake_request_id"]

        self.event_publisher = EventPublisher()

    @classmethod
    def from_chain_request_id(cls, chain_request_id: str) -> 'ChainCoordinator':
        """
        Loads a ChainCoordinator object from a chain request ID

        Keyword Arguments:
        chain_request_id -- The ID of the chain request to load
        """
        chain_requests = LakeRequestChainsClient()

        chain_request = chain_requests.get(lake_chain_request_id=chain_request_id, consistent_read=True)

        return cls(
            chain=chain_request.original_request_body,
            executed_requests=chain_request.executed_requests,
        )

    def get_referenced_id(self, reference: str) -> str:
        """
        Returns the id of the referenced request

        Keyword Arguments:
        reference -- The reference to load
        """
        reference_name = reference[len(self.REFERENCE_PREFIX):]

        return self.executed_requests.get(reference_name)

    def get_referenced_name(self, reference: str) -> str:
        """
        Returns the name of the referenced request

        Keyword Arguments:
        reference -- The reference to load
        """
        return reference[len(self.REFERENCE_PREFIX):]

    def has_executed(self, request_name: str) -> bool:
        """
        Returns True if the given request has already been executed

        Keyword Arguments:
        request_name -- The name of the request to check
        """
        return request_name in self.executed_requests

    def is_reference(self, reference: str) -> bool:
        """
        Returns True if the given reference is a reference to a previously executed request
        """
        return reference.startswith(self.REFERENCE_PREFIX)

    def _next_available_request_group(self) -> List[ObjectBody]: 
        """
        Returns the next group of lake requests that can be executed
        """
        next_group = []

        for request in self.chain["requests"]:
            logging.debug(f"Checking request for availability to kick-off: {request['name']}")

            if self.has_executed(request["name"]):
                continue

            request_obj = request

            updated_instructions = []

            for lookup_instruction in request["lookup_instructions"]:

                # If the request is not a direct entry, it doesn't support references
                if lookup_instruction["request_type"] != "DIRECT_ENTRY":
                    updated_instructions.append(lookup_instruction)

                    continue

                if self.is_reference(lookup_instruction["entry_id"]):

                    referenced_name = self.get_referenced_name(lookup_instruction["entry_id"])

                    if self.has_executed(request_name=referenced_name):
                        updated_attr = {"entry_id": self.get_referenced_id(lookup_instruction["entry_id"])}

                        updated_instructions.append(lookup_instruction.map_to(
                            new_schema=DirectEntryLookupSchema,
                            updated_attributes=updated_attr
                        ))

                        next_group.append()

                    else:
                        # If the referenced request has not been executed, then we can't execute this request yet
                        break

            request_obj = request_obj.map_to(
                new_schema=LakeChainRequestSchema,
                updated_attributes={"lookup_instructions": updated_instructions}
            ) 

            next_group.append(request_obj)

        return next_group

    def execute_next(self, parent_job_id: str, parent_job_type: str) -> int:
        """
        Executes the next group of requests in the chain

        Keyword Arguments:
        parent_job_id -- The ID of the parent job
        parent_job_type -- The type of the parent job

        Returns the number of requests that were executed
        """
        next_group = self._next_available_request_group()

        if not next_group:
            logging.debug("No more requests to execute ... exiting")

            return 0

        jobs = JobsClient()

        parent_job = jobs.get(job_id=parent_job_id, job_type=parent_job_type)

        lake_requests = LakeRequestsClient()

        running_requests = LakeRequestChainRunningRequestsClient()

        for request in next_group:
            child_job = parent_job.create_child(job_type="LAKE_REQUEST")

            lake_request = LakeRequest(
                job_id=child_job.job_id,
                job_type=child_job.job_type,
                last_known_stage=LakeRequestStage.VALIDATING,
                lookup_instructions=request.get('lookup_instructions'),
                processing_instructions=request.get('processing_instructions'),
                response_config=request.get('response_config'),
            )

            lake_requests.put(lake_request)

            req_obj = ObjectBody(
                body={
                    "job_id": child_job.job_id,
                    "job_type": child_job.job_type,
                    "lake_request_id": lake_request.lake_request_id,
                    "lookup_instructions": request.get('lookup_instructions'),
                    "processing_instructions": request.get('processing_instructions'),
                    "response_config": request.get('response_config'),
                },
                schema=LakeRequestEventBodySchema,
            )

            self.event_publisher.submit(
                event=EventBusEvent(
                    body=req_obj.to_dict(),
                    event_type=req_obj.get("event_type"),
                )
            )

            running_request = LakeRequestChainRunningRequest(
                chain_request_id=self.chain_request_id,
                chain_request_name=request["name"],
                lake_request_id=lake_request.lake_request_id,
            )

            running_requests.put(running_request)

        return len(next_group)

    def to_dict(self):
        """
        Returns the ChainCoordinator object as a dictionary
        """
        return {
            "chain": self.chain.to_dict(),
            "executed_requests": self.executed_requests,
        }


_FN_NAME = 'omnilake.services.request_manager.chain_coordinator.handle_lake_response'


@fn_event_response(function_name=_FN_NAME, exception_reporter=ExceptionReporter(),
                   logger=Logger(_FN_NAME))
def handle_lake_response(event, context):
    """
    Handles all lake request completion events
    """
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(
        body=source_event.body,
        schema=LakeCompletionEventBodySchema,
    )

    lake_request_id = event_body["lake_request_id"]

    running_requests = LakeRequestChainRunningRequestsClient()

    running_request = running_requests.get(lake_request_id=lake_request_id)

    if not running_request:
        logging.debug(f"Could not find running request for lake request id {lake_request_id}, no chain to coordinate")

        return

    chains = LakeRequestChainsClient()

    remaining_num_procs = chains.record_lake_request_results(
        lake_request_id=lake_request_id,
        reference_name=running_request.chain_request_name,
        request_chain_id=running_request.chain_request_id,
    )

    # Exit if there are still other lake requests to wait on
    if remaining_num_procs != 0:
        logging.debug("Still waiting on other lake requests to complete ... nothing to do")

        return

    coordinator = ChainCoordinator.from_chain_request_id(running_request.chain_request_id)

    # Execute the next group of requests in the chain
    logging.debug("Executing next group of requests in chain")

    num_of_executed = coordinator.execute_next(
        parent_job_id=event_body["job_id"],
        parent_job_type=event_body["job_type"],
    )

    if num_of_executed == 0:
        logging.debug("No more requests to execute ... cleaning up chain")

        jobs = JobsClient()

        parent_job = jobs.get(job_id=event_body["job_id"], job_type=event_body["job_type"])

        parent_job.status = JobStatus.COMPLETED

        parent_job.ended = datetime.now(tz=utc_tz)

        chain = chains.get(lake_chain_request_id=running_request.chain_request_id)

        chain.ended = datetime.now(tz=utc_tz)

        chain.status = LakeRequestStatus.COMPLETED

        chains.put(chain)

        return



_FN_NAME = 'omnilake.services.request_manager.chain_coordinator.initiate_chain'


@fn_event_response(function_name=_FN_NAME, exception_reporter=ExceptionReporter(),
                   logger=Logger(_FN_NAME))

def handle_initiate_chain(event, context):
    """
    Handles incoming new chain requests
    """
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(
        body=source_event.body,
        schema=LakeChainRequestEventBodySchema,
    )

    # Initialize the chain by creating a new table object
    chains_client = LakeRequestChainsClient()

    chain_request = LakeRequestChain(
        chain=event_body["requests"],
        chain_request_id=event_body["chain_request_id"],
        job_id=event_body["job_id"],
        job_type=event_body["job_type"],
    )

    chains_client.put(chain_request)

    logging.debug("Chain request saved")

    # Create a new ChainCoordinator object to handle the chain
    logging.debug("Initializing new ChainCoordinator object")

    coordinator = ChainCoordinator(
        chain=event_body["requests"],
        chain_request_id=event_body["chain_request_id"],
    )

    # Execute the next group of requests in the chain
    logging.debug("Executing next group of requests in chain")

    coordinator.execute_next(
        parent_job_id=event_body["job_id"],
        parent_job_type=event_body["job_type"],
    )