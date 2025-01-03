from da_vinci.core.immutable_object import (
    ObjectBodySchema,
    SchemaAttribute,
    SchemaAttributeType,
)


class BasicArchiveLookupObjectSchema(ObjectBodySchema):
    """Basic Archive Lookup Object Schema"""
    attributes=[
        SchemaAttribute(
            name='archive_id',
            type=SchemaAttributeType.STRING,
            required=True,
        ),
        SchemaAttribute(
            name='max_entries',
            type=SchemaAttributeType.NUMBER,
            required=False,
        ),
        SchemaAttribute(
            name='prioritize_tags',
            type=SchemaAttributeType.STRING_LIST,
        ),
    ]


class BasicArchiveProvisionObjectSchema(ObjectBodySchema):
    """Basic Archive Provision Object Schema"""
    attributes=[
        SchemaAttribute(
            name='retain_latest_originals_only',
            type=SchemaAttributeType.BOOLEAN,
            default_value=True,
            required=False,
        ),

        SchemaAttribute(
            name='tag_hint_instructions',
            type=SchemaAttributeType.STRING,
            required=False,
        ),

        SchemaAttribute(
            name='tag_model_id',
            type=SchemaAttributeType.STRING,
            required=False,
        ),
    ]