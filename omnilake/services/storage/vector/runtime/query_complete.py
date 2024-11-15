import logging

from datetime import datetime, UTC as utc_tz
from typing import Dict, List

from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.event_definitions import (
    InformationRequestBody,
    QueryCompleteBody,
)
from omnilake.internal_lib.naming import EntryResourceName, OmniLakeResourceName

from omnilake.tables.archive_entries.client import ArchiveEntriesClient
from omnilake.tables.entries.client import Entry, EntriesClient
from omnilake.tables.jobs.client import JobsClient, JobStatus
from omnilake.tables.vector_store_queries.client import VectorStoreQueryClient


def remove_source_duplicates(entries: List[Entry]) -> List[Entry]:
    """
    Remove all entries that are duplicates of the source. Favor the entry with the latest effective date.

    Keyword arguments:
    entries -- The entries to remove duplicates from.
    """
    existing_source_entries = {}

    ids_to_remove = set()

    entries_client = EntriesClient()

    for idx, entry in enumerate(entries):
        entry_global_obj  = entries_client.get(entry.entry_id)

        original_of_source = entry_global_obj.original_of_source

        if not original_of_source:
            continue

        if original_of_source not in existing_source_entries:
            existing_source_entries[original_of_source] = {
                'list_id': idx,
                'effective_date': entry_global_obj.effective_on,
            }

            continue

        existing_entry_idx = existing_source_entries[original_of_source]['list_id']

        existing_entry_effective_date = existing_source_entries[original_of_source]['effective_date']

        existing_entry = entries[existing_entry_idx]

        if existing_entry_effective_date < entry_global_obj.effective_on:
            logging.debug(f'Removing duplicate source entry {existing_entry.entry_id} in favor of {entry.entry_id}.')

            ids_to_remove.add(existing_entry_idx)

            existing_source_entries[original_of_source] = idx

        else:
            logging.debug(f'Removing duplicate source entry {entry.entry_id} in favor of {existing_entry.entry_id}.')

            ids_to_remove.add(idx)

    return [entry for idx, entry in enumerate(entries) if idx not in ids_to_remove]


def sort_resource_names(archive_id: str, resource_names: List[EntryResourceName], target_tags: List[str]) -> List[Entry]:
    """
    Sort the entries based on the target tags.

    Keyword arguments:
    entries -- The entries to sort.
    target_tags -- The target tags to sort against.
    """
    entries = ArchiveEntriesClient()

    entries_to_sort = []

    for resource_name in resource_names:
        logging.debug(f'Fetching entry ID: {resource_name}')

        entry = entries.get(archive_id=archive_id, entry_id=resource_name.resource_id)

        logging.debug(f'Entry: {entry}')

        if not entry:
            raise ValueError(f'Could not find entry {resource_name.resource_id}')

        entries_to_sort.append(entry)

    # Find all entries that are originals of the same source and find the latest effective date
    # TODO: Possibly always look for the latest effective dated entry matching the source
    de_duplicated_entries = remove_source_duplicates(entries_to_sort)

    sorted_entries = sorted(
        de_duplicated_entries,
        key=lambda entry_obj: entry_obj.calculate_score(target_tags),
        reverse=True,
    )

    result = [EntryResourceName(sorted_entry.entry_id) for sorted_entry in sorted_entries]

    return result


@fn_event_response(exception_reporter=ExceptionReporter(), function_name='query_complete',
                     logger=Logger('omnilake.storage.vector.query_complete'))
def handler(event: Dict, context: Dict):
    """
    Handle the query complete event
    """
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = QueryCompleteBody(**source_event.body)

    vs_queries = VectorStoreQueryClient()

    vs_queries.add_resulting_resources(
        query_id=event_body.query_id,
        resulting_resources=event_body.resource_names,
    )

    query_info = vs_queries.get(event_body.query_id, consistent_read=True)

    if query_info.remaining_processes > 0:
        logging.info(f'Query {event_body.query_id} still has {query_info.remaining_processes} processes remaining.')

        return

    logging.info(f'Query {event_body.query_id} has completed.')

    query_info.completed_on = datetime.now(tz=utc_tz)

    vs_queries.put(query_info)

    resulting_resources = [OmniLakeResourceName.from_string(r) for r in query_info.resulting_resources]

    # Handle sorting of the responses if over the max_entries limit
    if query_info.max_entries < len(query_info.resulting_resources):
        logging.info(f'Sorting results for query {event_body.query_id}.')

        sorted_resources = sort_resource_names(
            archive_id=query_info.archive_id,
            resource_names=resulting_resources,
            target_tags=query_info.target_tags,
        )

        resulting_resources = sorted_resources[:query_info.max_entries]

        vs_queries.put(query_info)

    event_publisher = EventPublisher()

    information_request_body = InformationRequestBody(
        request_id=query_info.request_id,
        resource_names=list([str(res) for res in resulting_resources]),
        request_stage='QUERY_COMPLETE',
    )

    logging.debug(f'Submitting event: {information_request_body}')

    event_publisher.submit(
        event=source_event.next_event(
            body=information_request_body.to_dict(),
            event_type=InformationRequestBody.event_type,
        )
    )

    jobs = JobsClient()

    logging.info(f'Updating job {query_info.job_type}/{query_info.job_id} to COMPLETED.')

    job = jobs.get(job_type=query_info.job_type, job_id=query_info.job_id)

    job.status = JobStatus.COMPLETED

    job.ended = datetime.now(tz=utc_tz)

    jobs.put(job)

    logging.info(f'Job {job.job_type}/{job.job_id} has completed.')