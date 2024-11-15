'''
Archive API Declaration
'''
from typing import Optional

from da_vinci.event_bus.client import EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.api.runtime.construct import ChildAPI, Route

from omnilake.internal_lib.event_definitions import (
    CreateBasicArchiveBody,
    CreateVectorArchiveBody,
)
from omnilake.internal_lib.job_types import JobType

from omnilake.tables.archives.client import ArchivesClient
from omnilake.tables.jobs.client import Job, JobsClient


class ArchiveAPI(ChildAPI):
    routes = [
        Route(
            path='/create_archive',
            method_name='create_archive',
        ),
        Route(
            path='/describe_archive',
            method_name='describe_archive',
        ),
        Route(
            path='/update_archive',
            method_name='update_archive',
        ),
    ]

    def create_archive(self, archive_id: str, description: str, retain_latest_originals_only: Optional[bool] = True,
                       storage_type: Optional[str] = 'VECTOR', tag_hint_instructions: Optional[str] = None):
        """
        Create an archive

        Keyword arguments:
        archive_id -- The ID of the archive
        description -- The description of the archive
        retain_latest_originals_only -- Whether to retain only the latest originals
        storage_type -- The storage type of the archive
        tag_hint_instructions -- Instructions for tagging entries in the archive
        """
        archives = ArchivesClient()

        existing = archives.get(archive_id)

        if existing:
            return self.respond(
                body={"message": "Archive already exists"},
                status_code=400,
            )

        job = Job(job_type=JobType.CREATE_ARCHIVE)

        jobs = JobsClient()

        jobs.put(job)

        if storage_type == 'VECTOR':
            event = EventBusEvent(
                event_type=CreateVectorArchiveBody.event_type,
                body=CreateVectorArchiveBody(
                    archive_id=archive_id,
                    description=description,
                    job_id=job.job_id,
                    retain_latest_originals_only=retain_latest_originals_only,
                    tag_hint_instructions=tag_hint_instructions,
                ).to_dict(),
            )

        else:
            event = EventBusEvent(
                event_type=CreateBasicArchiveBody.event_type,
                body=CreateBasicArchiveBody(
                    archive_id=archive_id,
                    description=description,
                    job_id=job.job_id,
                    retain_latest_originals_only=retain_latest_originals_only,
                    tag_hint_instructions=tag_hint_instructions,
                ).to_dict(),
            )

        publisher = EventPublisher()

        publisher.submit(event)

        return self.respond(
            body=job.to_dict(json_compatible=True),
            status_code=201,
       )

    def describe_archive(self, archive_id: str):
        """
        Describe an archive

        Keyword arguments:
        archive_id -- The ID of the archive
        """
        archives = ArchivesClient()

        existing = archives.get(archive_id)

        if not existing:
            return self.respond(
                body={"message": "Archive does not exist"},
                status_code=404,
            )

        return self.respond(
            body=existing.to_dict(json_compatible=True),
            status_code=200,
        )

    def update_archive(self, archive_id: str, description: str, tag_hint_instructions: Optional[str]):
        """
        Update an archive

        Keyword arguments:
        archive_id -- The ID of the archive
        description -- The description of the archive
        tag_hint_instructions -- Instructions for tagging entries in the archive
        """
        archives = ArchivesClient()

        existing = archives.get(archive_id)

        if not existing:
            return self.respond(
                body={"message": "Archive does not exist"},
                status_code=404,
            )

        if description:
            existing.description = description


        if tag_hint_instructions:
            existing.tag_hint_instructions = tag_hint_instructions

        archives.put(existing)

        return self.respond(
            body=existing.to_dict(json_compatible=True),
            status_code=200,
        )