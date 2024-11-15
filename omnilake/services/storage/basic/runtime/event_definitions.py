"""Basic storage service event types"""

from dataclasses import dataclass

from omnilake.internal_lib.event_definitions import GenericEventBody


@dataclass
class BasicArchiveVacuum(GenericEventBody):
    archive_id: str
    entry_id: str