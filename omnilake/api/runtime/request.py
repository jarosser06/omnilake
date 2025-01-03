'''
Handles Lake Requests and Lake Chain Requests
'''
import logging

from da_vinci.core.immutable_object import (
    ObjectBody,
    ObjectBodySchema,
    SchemaAttribute,
    SchemaAttributeType,
)

from da_vinci.event_bus.client import EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.api.runtime.base import ChildAPI, Route

from omnilake.internal_lib.event_definitions import LakeRequestEventBodySchema

from omnilake.tables.jobs.client import Job, JobsClient
from omnilake.tables.lake_requests.client import (
    LakeRequest as LakeRequestObj,
    LakeRequestStage,
    LakeRequestsClient,
)


class SubmitLakeRequestSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='lookup_instructions',
            type=SchemaAttributeType.OBJECT_LIST,
        ),

        SchemaAttribute(
            name='processing_instructions',
            type=SchemaAttributeType.OBJECT,
        ),

        SchemaAttribute(
            name='response_config',
            type=SchemaAttributeType.OBJECT,
            default_value={},
            required=False,
        )
    ]


class SubmitLakeRequestChainSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='requests',
            type=SchemaAttributeType.OBJECT_LIST,
        ),
    ]


class DescribeLakeRequestSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='lake_request_id',
            type=SchemaAttributeType.STRING,
        )
    ]


class LakeRequestAPI(ChildAPI):
    routes = [
        Route(
            path='/describe_lake_request',
            method_name='describe_request',
            request_body_schema=DescribeLakeRequestSchema,
        ),

        Route(
            path='/submit_lake_request',
            method_name='submit_request',
            request_body_schema=SubmitLakeRequestSchema,
        ),

        Route(
            path='/submit_lake_request_chain',
            method_name='submit_request_chain',
            request_body_schema=SubmitLakeRequestSchema,
        ),
    ]

    def describe_request(self, request: ObjectBody):
        """
        Create an archive

        Keyword arguments:
        request -- The request body
        """
        lake_requests = LakeRequestsClient()

        lake_request_id = request["lake_request_id"]

        request_obj = lake_requests.get(lake_request_id=lake_request_id)

        if not request_obj:
            logging.debug(f'Lake request not found: {lake_request_id}')

            return self.respond(
                body={'message': "lake request not found"},
                status_code=404,
            )

        response_body = request_obj.to_dict(json_compatible=True)

        logging.debug(f'Describing request: {response_body}')

        return self.respond(
            body=response_body,
            status_code=200,
        )

    def submit_request(self, request: ObjectBody):
        """
        Handles Lake Request and Request Chain submission

        Keyword arguments:
        request -- The request body
        """
        job = Job(job_type='LAKE_REQUEST')

        jobs = JobsClient()

        jobs.put(job)

        lookup_instructions = request['lookup_instructions']

        processing_instructions = request['processing_instructions']

        response_config = request['response_config']

        lake_requests = LakeRequestsClient()

        # Populate initial lake request to obtain the request_id
        lake_request = LakeRequestObj(
            job_id=job.job_id,
            job_type=job.job_type,
            last_known_stage=LakeRequestStage.VALIDATING,
            lookup_instructions=lookup_instructions,
            processing_instructions=processing_instructions,
            response_config=response_config,
        )

        lake_requests.put(lake_request)

        event_body = ObjectBody(
            body={
                "job_id": job.job_id,
                "job_type": job.job_type,
                "lake_request_id": lake_request.lake_request_id,
                "lookup_instructions": lookup_instructions,
                "processing_instructions": processing_instructions,
                "response_config": response_config,
            },
            schema=LakeRequestEventBodySchema,
        )

        event = EventBusEvent(
            event_type=event_body.get("event_type"),
            body=event_body.to_dict(),
        )

        publisher = EventPublisher()

        publisher.submit(event)

        return self.respond(
            body={
                'job_id': job.job_id,
                'job_type': job.job_type,
                'lake_request_id': lake_request.lake_request_id,
            },
            status_code=201,
        )

    def submit_request_chain(self, request: ObjectBody):
        """
        Handles Lake Request and Request Chain submission

        Keyword arguments:
        request -- The request body
        """
        return self.respond(
            body={'message': "Not implemented"},
            status_code=401,
        )