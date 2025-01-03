'''
CDK Construct for registering an OmniLake Construct
'''
from typing import Dict, List, Optional, Set, Union

from constructs import Construct

from da_vinci.core.immutable_object import (
    ObjectBodySchema,
)

from da_vinci_cdk.constructs.base import custom_type_name
from da_vinci_cdk.constructs.dynamodb import DynamoDBItem

from omnilake.tables.registered_request_constructs.client import (
    ArchiveConstructSchemas,
    RegisteredRequestConstruct as RegisteredRequestConstructObj,
    RequestConstructType,
)


class RegisteredRequestConstruct(DynamoDBItem):
    """Registered Request Construct"""
    def __init__(self, registered_construct_type: Union[str, RequestConstructType], registered_type_name: str,
                 scope: Construct, additional_supported_operations: Optional[Union[List, Set]] = None, 
                 description: Optional[str] = None, schemas: Optional[Union[ObjectBodySchema, Dict]] = None):
        """
        Initialize the global setting item.

        Keyword Arguments:
            registered_construct_type -- The type of the construct.
            registered_type_name -- The name of the construct.
            scope -- The CDK scope.
            description -- The description of the construct.
            schemas -- The schemas of the construct.
            additional_supported_operations -- The additional supported operations.
        """
        base_construct_id = f'omnilake-{registered_construct_type}-{registered_type_name}'

        schema_dict = None

        if schemas:
            schema_dict = schemas

            if isinstance(schemas, ArchiveConstructSchemas):
                schema_dict = schemas.to_dict()


        super().__init__(
            construct_id=base_construct_id,
            custom_type_name=custom_type_name('Construct', prefix='OmniLake'),
            scope=scope,
            support_updates=True,
            table_object=RegisteredRequestConstructObj(
                registered_construct_type=registered_construct_type,
                registered_type_name=registered_type_name,
                description=description,
                schemas=schema_dict,
                additional_supported_operations=set(additional_supported_operations) if additional_supported_operations else None
            )
        )

    @classmethod
    def from_definition(cls, registered_construct: RegisteredRequestConstructObj, scope: Construct):
        """
        Initialize the global setting item.

        Keyword Arguments:
            registered_construct: The registered construct object.
            scope: The CDK scope.
        """
        return cls(
            registered_construct_type=registered_construct.registered_construct_type,
            registered_type_name=registered_construct.registered_type_name,
            description=registered_construct.description,
            scope=scope,
            schemas=registered_construct.schemas,
            additional_supported_operations=registered_construct.additional_supported_operations,
        )