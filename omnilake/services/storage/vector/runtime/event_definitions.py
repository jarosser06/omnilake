"""Vector storage service event types"""

from dataclasses import dataclass

from omnilake.internal_lib.event_definitions import GenericEventBody


@dataclass
class RequestMaintenanceModeBegin(GenericEventBody):
    archive_id: str
    job_id: str
    job_type: str


@dataclass
class RequestMaintenanceModeEnd(GenericEventBody):
    archive_id: str
    job_id: str
    job_type: str


@dataclass
class VectorArchiveVacuum(GenericEventBody):
    archive_id: str
    entry_id: str


@dataclass
class VectorStoreRebalancing(GenericEventBody):
    archive_id: str
    vector_store_id: str


@dataclass
class VectorStoreTagRecalculationFinished(GenericEventBody):
    parent_job_id: str
    parent_job_type: str