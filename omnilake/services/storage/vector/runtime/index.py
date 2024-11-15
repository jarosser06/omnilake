'''
Processes all requests to vectorize text data and store it in vector storage.
'''
import json
import logging

from datetime import datetime, UTC as utc_tz
from typing import Dict, List
from uuid import uuid4

import boto3
import lancedb

from da_vinci.core.global_settings import setting_value
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.clients import RawStorageManager
from omnilake.internal_lib.event_definitions import IndexVectorEntryBody, GenerateEntryTagsBody
from omnilake.internal_lib.job_types import JobType
from omnilake.internal_lib.naming import SourceResourceName

from omnilake.services.storage.vector.runtime.vector_storage import (
    choose_vector_stores,
    DocumentChunk,
)

from omnilake.tables.archives.client import ArchivesClient
from omnilake.tables.archive_entries.client import (
    ArchiveEntry,
    ArchiveEntriesClient,
    ArchiveEntriesScanDefinition,
)
from omnilake.tables.jobs.client import Job, JobsClient, JobStatus
from omnilake.tables.sources.client import SourcesClient
from omnilake.tables.vector_stores.client import VectorStoresClient
from omnilake.tables.vector_store_chunks.client import VectorStoreChunksClient, VectorStoreChunk
from omnilake.tables.vector_store_tags.client import VectorStoreTagsClient

from omnilake.services.storage.vector.runtime.event_definitions import (
    RequestMaintenanceModeEnd,
    VectorArchiveVacuum,
)


def text_chunker(text: str, max_chunk_length: int = 1000, overlap: int = 40) -> List[str]:
    '''
    Helper function for chunking text based on the max_chunk_length and overlap.

    Keyword arguments:
    text -- The text to chunk.
    max_chunk_length -- The maximum length of each chunk.
    overlap -- The overlap between chunks.
    '''
    # Initialize result
    result = []
    start = 0

    while start < len(text):
        end = min(start + max_chunk_length, len(text))
        chunk = text[start:end]

        # Append the chunk
        result.append(chunk)

        # Calculate new start position, considering overlap
        start += max_chunk_length - overlap

    return result


def chunk_text(text: str, max_chunk_length: int = 1000, overlap: int = 40) -> List[str]:
    """
    Chunk text into smaller pieces.

    Keyword Arguments:
    text -- The text to chunk.
    max_chunk_length -- The maximum length of each chunk.
    overlap -- The overlap between chunks.
    """
    return text_chunker(text, max_chunk_length, overlap)


def get_embeddings(text: str):
    """
    Get embeddings for a given text.

    Keyword arguments:
    text -- The text to get embeddings for.
    """
    bedrock = boto3.client(service_name='bedrock-runtime')
    
    body = json.dumps({
        "texts": [text],
        "input_type": "search_document"
    })
    
    response = bedrock.invoke_model(
        modelId="cohere.embed-multilingual-v3",
        contentType="application/json",
        accept="application/json",
        body=body
    )
    
    response_body = json.loads(response['body'].read())

    logging.debug(f"Embedding response: {response_body}")

    embedding = response_body['embeddings'][0]

    return embedding


def generate_vector_data(entry_id: str, text_chunks: List[str]) -> List[DocumentChunk]:
    """
    Generate vector data for a given text.

    Keyword arguments:
    entry_id -- The entry ID to associate with the vector data.
    text_chunks -- The text chunks to generate vector data for.
    """
    embeddings = []

    for chunk in text_chunks:
        embedding_results = get_embeddings(chunk)

        embeddings.append(embedding_results)

    data = []

    for chunk, embed in zip(text_chunks, embeddings):
        data.append({
            'entry_id': entry_id,
            'chunk_id': str(uuid4()),
            'vector': embed 
        })

    return data


def is_latest_entry_for_original(source_resource_name: str, entry_id: str) -> bool:
    """
    Validate that the latest entry for the given original source is the entry being processed.

    Keyword arguments:
    source_resource_name -- The source resource name to validate.
    """
    sources_client = SourcesClient()

    source_rn = SourceResourceName.from_resource_name(source_resource_name)

    source = sources_client.get(source_type=source_rn.resource_id.source_type, source_id=source_rn.resource_id.source_id)

    return source.latest_content_entry_id == entry_id


@fn_event_response(function_name="vector_indexer", exception_reporter=ExceptionReporter(), logger=Logger("omnilake.storage.vector.index_entry"))
def handler(event: Dict, context: Dict):
    """
    Vectorizes the text data and stores it in vector storage.

    Keyword arguments:
    event -- The event data.
    context -- The context data.
    """
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = IndexVectorEntryBody(**source_event.body)

    jobs = JobsClient()

    if not event_body.job_id:
        job = Job(job_type=event_body.job_type)

        jobs.put(job)

    else:
        job = jobs.get(job_type=event_body.job_type, job_id=event_body.job_id)

    vectorize_job = job.create_child(job_type=JobType.INDEX_ENTRY)

    if vectorize_job.status == JobStatus.FAILED:
        logging.info(f"Job {vectorize_job.job_id} failed, not finishing vectorization")

    vectorize_job.status = JobStatus.IN_PROGRESS

    vectorize_job.started = datetime.now(utc_tz)

    entries = ArchiveEntriesClient()

    entry_obj = entries.get(archive_id=event_body.archive_id, entry_id=event_body.entry_id)

    if not entry_obj:
        effective_on = event_body.effective_on

        if effective_on:
            effective_on = datetime.fromisoformat(effective_on)

        entry_obj = ArchiveEntry(
            archive_id=event_body.archive_id,
            effective_on=effective_on,
            entry_id=event_body.entry_id,
            original_of_source=event_body.original_of_source,
            tags=[],
        )

        entries.put(entry_obj)

    archives_client = ArchivesClient()

    archive = archives_client.get(event_body.archive_id)

    if archive.retain_latest_originals_only and entry_obj.original_of_source:
        if not is_latest_entry_for_original(event_body.original_of_source, event_body.entry_id):
            logging.debug(f"Entry {event_body.entry_id} is not the latest entry for original source {event_body.original_of_source} ... skipping indexing")

            return

    storage_mgr = RawStorageManager()

    # Retrieve the entry content from the storage manager
    entry_content = storage_mgr.get_entry(event_body.entry_id)

    if 'message' in entry_content.response_body:
        raise Exception(f"Error retrieving entry content: {entry_content.response_body['message']}")

    if not entry_obj.tags:
        logging.info(f"Entry {event_body.entry_id} has no tags, sending generate_tags event")

        event_publisher = EventPublisher()

        event_publisher.submit(
            event=source_event.next_event(
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

    # Get the max chunk length and overlap from the settings
    max_chunk_length = setting_value(namespace='vector_storage', setting_key='max_chunk_length')

    chunk_overlap = setting_value(namespace='vector_storage', setting_key='chunk_overlap')

    # Chunk the text
    text_chunks = chunk_text(entry_content.response_body['content'], max_chunk_length, chunk_overlap)

    # Generate the vector data
    data = generate_vector_data(event_body.entry_id, text_chunks=text_chunks)

    # Connect to the vector storage
    vector_bucket = setting_value(namespace='vector_storage', setting_key='vector_store_bucket')

    db = lancedb.connect(f's3://{vector_bucket}')

    # Choose the appropriate vector store
    logging.debug(f"Entry tags: {entry_obj.tags}")

    if event_body.vector_store_id:
        logging.debug(f"Using provided vector store ID: {event_body.vector_store_id}")

        vector_store_id = event_body.vector_store_id

    else:
        vector_store_id = choose_vector_stores(event_body.archive_id, entry_obj.tags)[0]

    logging.info(f"Selected vector store: {vector_store_id}")

    vector_stores = VectorStoresClient()

    vector_store_obj = vector_stores.get(event_body.archive_id, vector_store_id)

    vector_table = db.open_table(name=vector_store_obj.vector_store_name)

    vector_table.add(data)

    chunk_meta_client = VectorStoreChunksClient()

    logging.info(f"Adding {len(data)} chunks to vector store {vector_store_id}")

    for chunk in data:
        chunk_meta = VectorStoreChunk(
            archive_id=event_body.archive_id,
            entry_id=chunk['entry_id'],
            chunk_id=chunk['chunk_id'],
            vector_store_id=vector_store_id,
        )

        chunk_meta_client.put(chunk_meta)

    logging.info(f"Saved {len(data)} chunks to vector store {vector_store_id}")

    # Update the vector store stats
    vector_store_obj.total_entries += 1

    vector_store_obj.total_entries_last_calculated = datetime.now(utc_tz)

    vector_stores.put(vector_store_obj)

    new_tags = set(entry_obj.tags)

    vector_store_tags = VectorStoreTagsClient()

    vector_store_tags.add_vector_store_to_tags(
        archive_id=event_body.archive_id,
        vector_store_id=vector_store_id,
        tags=list(new_tags)
    )

    # Update the job statuses and close them out
    vectorize_job.status = JobStatus.COMPLETED

    vectorize_job.ended = datetime.now(utc_tz)

    jobs.put(vectorize_job)

    job.status = JobStatus.COMPLETED

    job.ended = datetime.now(utc_tz)

    jobs.put(job)

    event_publisher = EventPublisher()

    logging.info(f"Vectorization complete for entry {event_body.entry_id} .. sending end maintenance mode request")

    # Always send the end maintenance mode request, maintenance mode will only be ended if it is active
    event_publisher.submit(
        event=source_event.next_event(
            body=RequestMaintenanceModeEnd(
                archive_id=event_body.archive_id,
                job_id=job.job_id,
                job_type=job.job_type,
            ).to_dict(),
            event_type='end_maintenance_mode',
        )
    )

    if not archive.retain_latest_originals_only or not entry_obj.original_of_source:
        logging.debug(f"Not matching conditions for vacuuming {archive.archive_id} ... skipping vacuum check")

        return

    if entry_obj.original_of_source:
        scan_def = ArchiveEntriesScanDefinition()

        scan_def.add('original_of_source', 'equal', entry_obj.original_of_source) 

        archive_entries_client = ArchiveEntriesClient()

        archive_entries = archive_entries_client.full_scan(scan_def)

        for archive_entry in archive_entries:
            if archive_entry.entry_id == entry_obj.entry_id:
                logging.debug(f"Skipping processed entry")

                continue

            logging.debug(f"Deleting entry index for entry {archive_entry.entry_id} in archive {archive_entry.archive_id}")

            event_publisher.submit(
                event=source_event.next_event(
                    body=VectorArchiveVacuum(
                        archive_id=event_body.archive_id,
                        entry_id=archive_entry.entry_id,
                    ).to_dict(),
                    event_type='vector_vacuum_request',
                )
            )

            archive_entries_client.delete(archive_entry)

            logging.debug(f"Deleted entry index for entry {archive_entry.entry_id} in archive {archive_entry.archive_id}")