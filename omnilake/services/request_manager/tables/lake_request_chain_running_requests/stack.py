from constructs import Construct

from da_vinci_cdk.constructs.dynamodb import DynamoDBTable
from da_vinci_cdk.stack import Stack

from omnilake.services.request_manager.tables.lake_request_chain_running_requests.client import (
    LakeRequestChainRunningRequest,
)


class LakeRequestChainRunningRequestsTable(Stack):
    def __init__(self, app_name: str, deployment_id: str,
                 scope: Construct, stack_name: str):
        super().__init__(
            app_name=app_name,
            deployment_id=deployment_id,
            scope=scope,
            stack_name=stack_name
        )

        self.table = DynamoDBTable.from_orm_table_object(
            scope=self,
            table_object=LakeRequestChainRunningRequest,
        )