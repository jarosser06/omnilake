'''Index an entry into an archive.'''
import logging

from datetime import datetime, UTC as utc_tz
from typing import Dict

from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.clients import RawStorageManager
from omnilake.internal_lib.event_definitions import IndexBasicEntryBody, GenerateEntryTagsBody
from omnilake.internal_lib.naming import SourceResourceName

from omnilake.tables.archives.client import ArchivesClient
from omnilake.tables.archive_entries.client import (
    ArchiveEntry,
    ArchiveEntriesClient,
    ArchiveEntriesScanDefinition,
)
from omnilake.tables.jobs.client import Job, JobsClient, JobStatus
from omnilake.tables.sources.client import SourcesClient


def is_latest_entry_for_original(source_resource_name: str, entry_id: str) -> bool:
    """
    Validate that the latest entry for the given original source is the entry being processed.

    Keyword arguments:
    source_resource_name -- The source resource name to validate.
    """
    sources_client = SourcesClient()

    source_rn = SourceResourceName.from_resource_name(source_resource_name)

    source = sources_client.get(source_type=source_rn.resource_id.source_type, source_id=source_rn.resource_id.source_id)

    logging.debug(f"Checking if entry {entry_id} is the latest entry for original source {source_resource_name}")

    return source.latest_content_entry_id == entry_id


@fn_event_response(function_name="basic_indexer", exception_reporter=ExceptionReporter(),
                   logger=Logger("omnilake.storage.basic.index_entry"))
def handler(event: Dict, context: Dict):
    """
    Vectorizes the text data and stores it in vector storage.

    Keyword arguments:
    event -- The event data.
    context -- The context data.
    """
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = IndexBasicEntryBody(**source_event.body)

    jobs = JobsClient()

    if not event_body.job_id:
        job = Job(job_type=event_body.job_type)

        jobs.put(job)

    else:
        job = jobs.get(job_type=event_body.job_type, job_id=event_body.job_id)

    archives_client = ArchivesClient()

    archive = archives_client.get(event_body.archive_id)

    if archive.retain_latest_originals_only and event_body.original_of_source:
        if is_latest_entry_for_original(event_body.original_of_source, event_body.entry_id):
            logging.debug(f"Entry {event_body.entry_id} is the latest entry for original source {event_body.original_of_source} ... continuing indexing")

            scan_def = ArchiveEntriesScanDefinition()

            scan_def.add('original_of_source', 'equal', event_body.original_of_source) 

            archive_entries_client = ArchiveEntriesClient()

            archive_entries = archive_entries_client.full_scan(scan_def)

            for archive_entry in archive_entries:
                if archive_entry.entry_id == event_body.entry_id:
                    logging.debug(f"Skipping processed entry")

                    continue

                archive_entries_client.delete(archive_entry)

                logging.debug(f"Deleted entry index for entry {archive_entry.entry_id} in archive {archive_entry.archive_id}")

        else:
            logging.debug(f"Entry {event_body.entry_id} is not the latest entry for original source {event_body.original_of_source} ... skipping indexing")

            job.status = JobStatus.COMPLETED

            job.ended = datetime.now(utc_tz)

            jobs.put(job)

            return

    entries = ArchiveEntriesClient()

    entry_obj = entries.get(archive_id=event_body.archive_id, entry_id=event_body.entry_id)

    if not entry_obj:
        entry_obj = ArchiveEntry(
            archive_id=event_body.archive_id,
            entry_id=event_body.entry_id,
            effective_on=datetime.fromisoformat(event_body.effective_on),
            original_of_source=event_body.original_of_source,
            tags=[],
        )

        entries.put(entry_obj)

    storage_mgr = RawStorageManager()

    # Retrieve the entry content from the storage manager
    entry_content = storage_mgr.get_entry(event_body.entry_id)

    if 'message' in entry_content.response_body:
        raise Exception(f"Error retrieving entry content: {entry_content.response_body['message']}")

    if not entry_obj.tags:
        logging.info(f"Entry {event_body.entry_id} has no tags, sending generate_tags event")

        event_publisher = EventPublisher()

        event_publisher.submit(
            event=EventBusEvent(
                body=GenerateEntryTagsBody(
                    archive_id=event_body.archive_id,
                    entry_id=event_body.entry_id,
                    callback_body=event_body.to_dict(),
                    content=entry_content.response_body['content'],
                    parent_job_id=job.job_id,
                    parent_job_type=job.job_type,
                ).to_dict(),
                event_type='generate_entry_tags',
            )
        )

        return

    job.status = JobStatus.COMPLETED

    job.ended = datetime.now(utc_tz)

    jobs.put(job)