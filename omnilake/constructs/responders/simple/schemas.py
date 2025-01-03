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
            name='goal',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='model_id',
            type=SchemaAttributeType.STRING,
            required=False,
        ),
    ]