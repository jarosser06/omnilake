from datetime import datetime
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional, Union


@dataclass
class RequestSuperclass:
    def to_dict(self):
        """
        Return the object as a dictionary.
        """
        return asdict(self)


@dataclass
class AddEntry(RequestSuperclass):
    content: str
    sources: List[str]
    archive_id: Optional[str] = None
    effective_on: Optional[str] = None # will be set to time of insertion if not provided
    original_source: Optional[str] = None
    summarize: Optional[bool] = False


@dataclass
class AddSource(RequestSuperclass):
    source_type: str
    source_arguments: Dict


@dataclass
class CreateArchive(RequestSuperclass):
    archive_id: str
    description: str
    retain_latest_originals_only: Optional[bool] = True # whether to retain only the latest original entries in the archive
    storage_type: Optional[str] = 'VECTOR' # VECTOR or BASIC
    tag_hint_instructions: Optional[str] = None # instructions for tagging entries ingested into the archive, only applies to VECTOR archives


@dataclass
class CreateSourceType(RequestSuperclass):
    name: str
    required_fields: List[str] # list of required field names, used to generate the source ID
    description: str = None


@dataclass
class DeleteEntry(RequestSuperclass):
    entry_id: str
    force: Optional[bool] = False


@dataclass
class DeleteSource(RequestSuperclass):
    source_id: str
    source_type: str
    force: Optional[bool] = False


@dataclass
class DescribeArchive(RequestSuperclass):
    archive_id: str


@dataclass
class DescribeEntry(RequestSuperclass):
    entry_id: str


@dataclass
class DescribeJob(RequestSuperclass):
    job_id: str
    job_type: str


@dataclass
class DescribeSource(RequestSuperclass):
    source_id: str
    source_type: str


@dataclass
class DescribeSourceType(RequestSuperclass):
    name: str


@dataclass
class DescribeRequest(RequestSuperclass):
    request_id: str


@dataclass
class GetEntry(RequestSuperclass):
    entry_id: str


@dataclass
class IndexEntry(RequestSuperclass):
    archive_id: str
    entry_id: str


@dataclass
class BasicArchiveInformationRequest:
    archive_id: str
    max_entries: int
    evaluation_type: str = 'INCLUSIVE' # Only one supported type
    prioritize_tags: Optional[List[str]] = None # These are calculated by the system if not provided
    request_type: str = 'BASIC'


@dataclass
class VectorArchiveInformationRequest:
    archive_id: str
    max_entries: int # Must always be set by the requester
    evaluation_type: str = 'EXCLUSIVE' # Only one supported type
    query_string: str = None # required if the evaluation type is EXCLUSIVE
    prioritize_tags: Optional[List[str]] = None # These are calculated by the system if not provided
    request_type: str = 'VECTOR'


@dataclass
class InformationRequest(RequestSuperclass):
    goal: str
    requests: List[Union[Dict, BasicArchiveInformationRequest, VectorArchiveInformationRequest]]
    include_source_metadata: Optional[bool] = False
    resource_names: Optional[List[str]] = None
    responder_model_id: Optional[str] = None # system default used if not provided
    responder_prompt: Optional[str] = None # system default used if not provided
    summarization_algorithm: Optional[str] = 'STANDARD'
    summarization_prompt: Optional[str] = None # system default used if not provided
    summarization_model_id: Optional[str] = None # system default used if not provided

    def __post_init__(self):
        normalized_requests = []

        for req in self.requests:
            if isinstance(req, dict):
                normalized_requests.append(req)

            else:
                normalized_requests.append(asdict(req))

        self.requests = normalized_requests


@dataclass
class ScoreResponse(RequestSuperclass):
    request_id: str
    score: float
    score_comment: Optional[str] = None


@dataclass
class UpdateArchive(RequestSuperclass):
    archive_id: str
    description: str = None
    tag_hint_instructions: Optional[str] = None


@dataclass
class UpdateEntry(RequestSuperclass):
    entry_id: str
    content: str