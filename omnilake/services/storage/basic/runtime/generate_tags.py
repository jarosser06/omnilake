'''
Handles the processing of new entries and adds them to the storage.
'''
import logging

from typing import Dict, Optional, Tuple

from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.ai import AI, ModelIDs, AIInvocationResponse
from omnilake.internal_lib.ai_insights import (
    AIResponseDefinition,
    AIResponseInsightDefinition,
    ResponseParser,
)
from omnilake.internal_lib.event_definitions import (
    GenerateEntryTagsBody,
)

from omnilake.tables.archives.client import ArchivesClient
from omnilake.tables.archive_entries.client import ArchiveEntriesClient
from omnilake.tables.jobs.client import JobsClient


def extract_tags(content: str, tag_hint: Optional[str] = None, tag_model_id: Optional[str] = None,
                 tag_model_params: Optional[Dict] = None) -> Tuple[Dict, AIInvocationResponse]:
    """
    Uses AI to extract tags from the content.

    Keyword arguments:
    content -- The content to extract insights from
    tag_hint -- Special tagging instructions
    tag_model_id -- The model ID used for tagging
    tag_model_params -- The model parameters used for tagging
    """
    ai = AI()

    prompt_definition = """Extract relevant tags from the given content, focusing on:

- Proper names (people, places, organizations, products)
- Specific categories or themes
- Key concepts or topics
- Business categories or industries
- Subjects or disciplines (e.g., science, history, art)
- Time periods or eras
- Emotions or sentiments expressed
- Technical terms or jargon
- Cultural references
- Target audience or demographic

Guidelines:

- Provide tags as a comma-separated list
- Include both specific and broader tags where appropriate
- Aim for concise, descriptive tags (1-3 words each)
- Prioritize tags that would be most useful for categorization or search purposes
- Consider the context and main focus of the content when selecting tags
- If applicable, include tags in different languages that are relevant to the content
- Aim to capture the main themes rather than every minor detail

Tagging approach:

- First, read through the entire content to understand the overall context
- Identify the primary topic or theme
- Extract tags based on the categories listed above
- Review and refine the tag list, ensuring a balanced representation of the content"""

    if tag_hint:
        prompt_definition = f"{prompt_definition}\n\nSPECIAL TAGGING INSTRUCTIONS: {tag_hint}"

    response_definition = AIResponseDefinition(
        insights=[
            AIResponseInsightDefinition(
                name="tags",
                definition=prompt_definition,
            ),
        ]
    )

    prompt = response_definition.to_prompt(content)

    model_params = tag_model_params or {}

    result = ai.invoke(
        model_id=tag_model_id or ModelIDs.HAIKU,
        prompt=prompt,
        **model_params,
    )

    parser = ResponseParser()

    parser.feed(result.response)

    return parser.parsed_insights(), result


@fn_event_response(function_name='entry_tag_extraction', exception_reporter=ExceptionReporter(), logger=Logger("omnilake.storage.vector.entry_tag_extraction"))
def handler(event: Dict, context: Dict):
    """
    Processes the new entries and adds them to the storage.
    """
    source_event = EventBusEvent.from_lambda_event(event)

    event_body = GenerateEntryTagsBody(**source_event.body)

    jobs = JobsClient()

    job = jobs.get(job_type=event_body.parent_job_type, job_id=event_body.parent_job_id)

    tag_extraction_job = job.create_child(job_type='ENTRY_TAG_EXTRACTION')

    entries = ArchiveEntriesClient()

    with jobs.job_execution(tag_extraction_job, fail_all_parents=True):

        entry = entries.get(archive_id=event_body.archive_id, entry_id=event_body.entry_id)

        archives = ArchivesClient()

        archive = archives.get(archive_id=event_body.archive_id)

        insights, invocation_resp = extract_tags(
            content=event_body.content,
            tag_hint=archive.tag_hint_instructions,
            tag_model_id=archive.tag_model_id,
            tag_model_params=archive.tag_model_params,
        )

        logging.debug(f"Invocation response: {invocation_resp}")

        tag_extraction_job.ai_statistics.invocations.append(invocation_resp.statistics)

        logging.debug(f"Extracted insights: {insights}")

        entry.tags = [tag.lower().strip() for tag in insights['tags'].split(',')]

        entries.put(entry)

        event_publisher = EventPublisher()

        event_publisher.submit(
            event=source_event.next_event(
                body=event_body.callback_body,
                event_type=event_body.callback_body['event_type'],
            )
        )