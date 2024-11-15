'''
Basic Archive Provisioner

This function is responsible for creating a new archive
'''
import logging

from datetime import datetime, UTC as utz_tz
from typing import Dict

from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.event_definitions import CreateVectorArchiveBody
from omnilake.internal_lib.job_types import JobType

from omnilake.tables.archives.client import Archive, ArchivesClient, ArchiveStatus
from omnilake.tables.jobs.client import Job, JobsClient, JobStatus


@fn_event_response(exception_reporter=ExceptionReporter(), function_name='basic_archive_provisioner',
                   logger=Logger('omnilake.storage.basic.provisioner'))
def handler(event: Dict, context: Dict) -> Dict:
    """
    Provisions a new archive
    """
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = CreateVectorArchiveBody(**source_event.body)

    jobs = JobsClient()

    job = jobs.get(job_type=JobType.CREATE_ARCHIVE, job_id=event_body.job_id)

    if not job:
        job = Job(job_id=event_body.job_id, job_type=JobType.CREATE_ARCHIVE)

    job.status = JobStatus.IN_PROGRESS

    job.started =  datetime.now(tz=utz_tz)

    jobs.put(job)

    archive = Archive(
        archive_id=event_body.archive_id,
        description=event_body.description,
        retain_latest_originals_only=event_body.retain_latest_originals_only,
        status=ArchiveStatus.ACTIVE,
        storage_type=event_body.storage_type,
        tag_hint_instructions=event_body.tag_hint_instructions,
        visibility=event_body.visibility,
    )

    archives = ArchivesClient()

    archives.put(archive)

    job.status = JobStatus.COMPLETED

    job.ended = datetime.now(tz=utz_tz)

    jobs.put(job)