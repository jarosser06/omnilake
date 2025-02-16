from da_vinci.core.immutable_object import (
    ObjectBodySchema,
    SchemaAttribute,
    SchemaAttributeType,
)


class RequestBodySchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='destination_archive_id',
            type=SchemaAttributeType.STRING,
            required=False,
        ),

        SchemaAttribute(
            name='append_text',
            type=SchemaAttributeType.STRING,
            required=False,
        ),

        SchemaAttribute(
            name='prepend_text',
            type=SchemaAttributeType.STRING,
            required=False,
        ),

        SchemaAttribute(
            name='separator',
            type=SchemaAttributeType.STRING,
            required=False,
            default_value="\n\n",
        )
    ]