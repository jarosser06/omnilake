"""
Handles the coordination of lookup responses for a given lake request.
"""
import logging

from da_vinci.core.immutable_object import ObjectBody
from da_vinci.core.logging import Logger

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from da_vinci.exception_trap.client import ExceptionReporter

from omnilake.internal_lib.event_definitions import (
    LakeRequestLookupResponse,
    LakeRequestInternalResponseEventBodySchema,
)

from omnilake.tables.lake_requests.client import LakeRequestsClient


_FN_NAME = 'omnilake.services.request_manager.lookup_coordination'


@fn_event_response(function_name=_FN_NAME, exception_reporter=ExceptionReporter(), logger=Logger(_FN_NAME))
def handler(event, context):
    """
    Response handler for the primitive lookup function.
    """
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(
        body=source_event.body,
        schema=LakeRequestLookupResponse,
    )

    lake_requests = LakeRequestsClient()

    lake_request_id = event_body.get('lake_request_id')

    remaining_lookups = lake_requests.add_lookup_results(lake_request_id=lake_request_id, results=event_body.get('entry_ids'))

    if remaining_lookups != 0:
        logging.debug(f'Not all lookups have completed for request {lake_request_id}')

        return

    lake_request = lake_requests.get(lake_request_id=lake_request_id, consistent_read=True)

    logging.debug(f'All lookups have completed for request {lake_request_id}')

    publisher = EventPublisher()

    response_body = ObjectBody(
        body={
            "entry_ids": list(lake_request.response_sources),
            "lake_request_id": lake_request_id,
        },
        schema=LakeRequestInternalResponseEventBodySchema,
    )

    publisher.submit(
        event=source_event.next_event(
            event_type=response_body['event_type'],
            body=response_body.to_dict(),
        )
    )