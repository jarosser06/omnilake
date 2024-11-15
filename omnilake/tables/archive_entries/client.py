from datetime import datetime, UTC as utc_tz
from typing import List, Optional, Union

from da_vinci.core.orm import (
    TableClient,
    TableObject,
    TableObjectAttribute,
    TableObjectAttributeType,
    TableScanDefinition,
)


class ArchiveEntry(TableObject):
    table_name = "archive_entries"

    description = "Entries aligned with arhives"

    partition_key_attribute = TableObjectAttribute(
        name="archive_id",
        attribute_type=TableObjectAttributeType.STRING,
        description="The ID of the archive",
    )

    sort_key_attribute = TableObjectAttribute(
        name="entry_id",
        attribute_type=TableObjectAttributeType.STRING,
        description="The ID of the entry",
    )

    attributes = [
        TableObjectAttribute(
            name="added_on",
            attribute_type=TableObjectAttributeType.DATETIME,
            description="The datetime the entry was added",
            default=lambda: datetime.now(utc_tz),
        ),

        TableObjectAttribute(
            name="effective_on",
            attribute_type=TableObjectAttributeType.DATETIME,
            description="The date and time the entry is effective on.",
            default=lambda: datetime.now(utc_tz),
        ),

        TableObjectAttribute(
            name="original_of_source",
            attribute_type=TableObjectAttributeType.STRING,
            description="The source resource name if the entry is original content of a source",
        ),

        TableObjectAttribute(
            name="tags",
            attribute_type=TableObjectAttributeType.STRING_LIST,
            description="The tags associated with the entry",
            default=[],
        ),
    ]

    @staticmethod
    def calculate_tag_match_percentage(object_tags: List[str], target_tags: List[str]) -> int:
        """
        Calculate the match percentage between the object's tags and the target tags.

        Keyword arguments:
        object_tags -- The list of tags to compare
        target_tags -- The list of tags to compare
        """
        matching_tags = set(object_tags) & set(target_tags)

        # Calculate the match percentage
        return len(matching_tags) / len(target_tags) * 100

    def calculate_score(self, target_tags: List[str]) -> int:
        """
        Calculate the match percentage based on the target tags.

        Keyword arguments:
        target_tags -- The target tags to calculate the score against.
        """
        tag_score = self.calculate_tag_match_percentage(
            object_tags=self.tags,
            target_tags=target_tags,
        )

        return tag_score


class ArchiveEntriesScanDefinition(TableScanDefinition):
    def __init__(self):
        super().__init__(table_object_class=ArchiveEntry)


class ArchiveEntriesClient(TableClient):
    def __init__(self, app_name: Optional[str] = None, deployment_id: Optional[str] = None):
        super().__init__(
            app_name=app_name,
            default_object_class=ArchiveEntry,
            deployment_id=deployment_id,
        )

    def delete(self, archive_entry: ArchiveEntry) -> None:
        """
        Delete an entry from the table.

        Keyword arguments:
        archive_id -- The ID of the archive
        entry_id -- The ID of the entry
        """
        return self.delete_object(table_object=archive_entry)

    def get(self, archive_id: str, entry_id: str) -> Optional[ArchiveEntry]:
        """
        Get an entry from the table.

        Keyword arguments:
        archive_id -- The ID of the archive
        entry_id -- The ID of the entry
        """
        return self.get_object(partition_key_value=archive_id, sort_key_value=entry_id)

    def put(self, entry: ArchiveEntry) -> None:
        """
        Put an entry into the table.

        Keyword arguments:
        entry -- The entry to put
        """
        return self.put_object(table_object=entry)