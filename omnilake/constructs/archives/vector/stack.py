from os import path

from aws_cdk import (
    Duration,
    RemovalPolicy,
)

from constructs import Construct

from aws_cdk.aws_iam import ManagedPolicy
from aws_cdk.aws_s3 import Bucket, BucketEncryption

from da_vinci.core.resource_discovery import ResourceType

from da_vinci_cdk.stack import Stack

from da_vinci_cdk.constructs.access_management import ResourceAccessRequest
from da_vinci_cdk.constructs.base import resource_namer
from da_vinci_cdk.constructs.global_setting import GlobalSetting, GlobalSettingType
from da_vinci_cdk.constructs.event_bus import EventBusSubscriptionFunction

from omnilake.tables.entries.stack import Entry, EntriesTable
from omnilake.tables.indexed_entries.stack import IndexedEntry, IndexedEntriesTable
from omnilake.tables.jobs.stack import Job, JobsTable
from omnilake.tables.provisioned_archives.stack import Archive, ProvisionedArchivesTable
from omnilake.tables.registered_request_constructs.cdk import (
    ArchiveConstructSchemas,
    RegisteredRequestConstructObj,
    RequestConstructType,
    RegisteredRequestConstruct
)
from omnilake.tables.registered_request_constructs.stack import RegisteredRequestConstructsTable
from omnilake.tables.sources.stack import Source, SourcesTable

from omnilake.services.ai_statistics_collector.stack import AIStatisticsCollectorStack

from omnilake.services.raw_storage_manager.stack import LakeRawStorageManagerStack

# Local Construct Imports
from omnilake.constructs.archives.vector.schemas import (
    VectorArchiveLookupObjectSchema,
    VectorArchiveProvisionObjectSchema,
)

from omnilake.constructs.archives.vector.tables.vector_stores.stack import (
    VectorStoresTable,
    VectorStore,
)
from omnilake.constructs.archives.vector.tables.vector_store_chunks.stack import (
    VectorStoreChunk,
    VectorStoreChunksTable,
)


class LakeConstructArchiveVectorStack(Stack):
    def __init__(self, app_name: str, app_base_image: str, architecture: str,
                 deployment_id: str, stack_name: str, scope: Construct):
        """
        Vector Archive manager stack for OmniLake.

        Keyword Arguments:
            app_name: The name of the app.
            app_base_image: The base image for the app.
            architecture: The architecture of the app.
            deployment_id: The deployment ID.
            stack_name: The name of the stack.
            scope: The scope of the stack.
        """

        super().__init__(
            app_name=app_name,
            app_base_image=app_base_image,
            architecture=architecture,
            requires_event_bus=True,
            requires_exceptions_trap=True,
            required_stacks=[
                AIStatisticsCollectorStack,
                EntriesTable,
                JobsTable,
                IndexedEntriesTable,
                LakeRawStorageManagerStack,
                ProvisionedArchivesTable,
                RegisteredRequestConstructsTable,
                SourcesTable,
                VectorStoresTable,
                VectorStoreChunksTable,
            ],
            deployment_id=deployment_id,
            scope=scope,
            stack_name=stack_name,
        )

        base_dir = self.absolute_dir(__file__)

        self.runtime_path = path.join(base_dir, 'runtime')

        schemas = ArchiveConstructSchemas(
            lookup=VectorArchiveLookupObjectSchema,
            provision=VectorArchiveProvisionObjectSchema,
        )

        self.registered_request_construct_obj = RegisteredRequestConstructObj(
            registered_construct_type=RequestConstructType.ARCHIVE,
            registered_type_name='VECTOR',
            description='Built-in vector archive provides a simple LanceDB-based archive stored in S3.',
            schemas=schemas,
            additional_supported_operations=set(['index']),
        )

        self.vector_store_bucket = Bucket(
            self,
            'vector-store-bucket',
            encryption=BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.vector_store_bucket_setting = GlobalSetting(
            namespace='omnilake::vector_storage',
            setting_key='vector_store_bucket',
            setting_value=self.vector_store_bucket.bucket_name,
            scope=self,
        )

        self.provisioner = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='vector_archive_provisioner',
            description='Provisions vector archives.',
            entry=self.runtime_path,
            event_type=self.registered_request_construct_obj.get_operation_event_name('provision'),
            index='provisioner.py',
            handler='handler',
            function_name=resource_namer('archive-vector-provisioner', scope=self),
            memory_size=512,
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name=Archive.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=Job.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=VectorStore.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
            ],
            scope=self,
            timeout=Duration.minutes(2),
        )

        self.vector_store_bucket.grant_read_write(self.provisioner.handler.function)

        self.entry_index = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='entry_vector_index_event',
            description='Indexes an entry into a vector store for the requested archive.',
            entry=self.runtime_path,
            event_type=self.registered_request_construct_obj.get_operation_event_name('index'),
            index='index.py',
            handler='handler',
            function_name=resource_namer('archive-vector-indexer', scope=self),
            memory_size=512,
            managed_policies=[
                ManagedPolicy.from_managed_policy_arn(
                    scope=self,
                    id='vector-index-amazon-bedrock-full-access',
                    managed_policy_arn='arn:aws:iam::aws:policy/AmazonBedrockFullAccess'
                ),
            ],
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name='event_bus',
                    resource_type=ResourceType.ASYNC_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_name=Archive.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read',
                ),
                ResourceAccessRequest(
                    resource_name=IndexedEntry.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=Job.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=Source.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read',
                ),
                ResourceAccessRequest(
                    resource_name='raw_storage_manager',
                    resource_type=ResourceType.REST_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_name=VectorStore.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=VectorStoreChunk.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                )
            ],
            scope=self,
            timeout=Duration.minutes(2),
        )

        self.vector_store_bucket.grant_read_write(self.entry_index.handler.function)

        self.chunk_overlap_setting = GlobalSetting(
            description="The percentage of overlap between chunks in a vector store.",
            namespace='omnilake::vector_storage',
            setting_key='chunk_overlap',
            setting_value=40,
            scope=self,
            setting_type=GlobalSettingType.INTEGER
        )

        # TODO: Add this to the lookup request body for Vector archives
        self.max_chunk_length_setting = GlobalSetting(
            description="The maximum length of a chunk in a vector store.",
            namespace='omnilake::vector_storage',
            setting_key='max_chunk_length',
            setting_value=1500,
            scope=self,
            setting_type=GlobalSettingType.INTEGER
        )

        self.lookup = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='vector_store_data_retrieval',
            description='Handles a query request for a vector store based archive.',
            entry=self.runtime_path,
            event_type=self.registered_request_construct_obj.get_operation_event_name('lookup'),
            index='lookup.py',
            handler='handler',
            function_name=resource_namer('archive-vector-data-retrieval', scope=self),
            memory_size=1024,
            managed_policies=[
                ManagedPolicy.from_managed_policy_arn(
                    scope=self,
                    id='query-request-amazon-bedrock-full-access',
                    managed_policy_arn='arn:aws:iam::aws:policy/AmazonBedrockFullAccess'
                ),
            ],
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name='event_bus',
                    resource_type=ResourceType.ASYNC_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_name=Archive.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read',
                ),
                ResourceAccessRequest(
                    resource_name=Entry.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read',
                ),
                ResourceAccessRequest(
                    resource_name=Job.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=IndexedEntry.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=VectorStore.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read',
                )
            ],
            scope=self,
            timeout=Duration.minutes(5),
        )

        self.vector_store_bucket.grant_read(self.lookup.handler.function)

        self.vacuum = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='vector_vacuum',
            description='Deletes old vector store chunks given an entry',
            entry=self.runtime_path,
            event_type='omnilake_archive_vector_vacuum_request',
            index='vacuum.py',
            handler='handler',
            function_name=resource_namer('archive-vector-vacuum', scope=self),
            memory_size=1024,
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name=IndexedEntry.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read',
                ),
                ResourceAccessRequest(
                    resource_name=Job.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=VectorStore.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=VectorStoreChunk.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
            ],
            scope=self,
            timeout=Duration.minutes(10),
        )

        self.vector_store_bucket.grant_read_write(self.vacuum.handler.function)

        self.entry_tag_generator_event = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='entry_tag_generator',
            description='Generates tags for an entry.',
            entry=self.runtime_path,
            event_type='omnilake_archive_vector_generate_entry_tags',
            index='generate_tags.py',
            handler='handler',
            function_name=resource_namer('archive-vector-entry-tag-generator', scope=self),
            memory_size=512,
            managed_policies=[
                ManagedPolicy.from_managed_policy_arn(
                    scope=self,
                    id='entry-tagger-amazon-bedrock-full-access',
                    managed_policy_arn='arn:aws:iam::aws:policy/AmazonBedrockFullAccess'
                ),
            ],
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name='ai_statistics_collector',
                    resource_type=ResourceType.REST_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_name='event_bus',
                    resource_type=ResourceType.ASYNC_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_name=Archive.table_name,
                    resource_type=ResourceType.TABLE,
                ),
                ResourceAccessRequest(
                    resource_name=IndexedEntry.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=Job.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
            ],
            scope=self,
            timeout=Duration.minutes(2),
        )

        # Register the Vector Archive Construct
        RegisteredRequestConstruct.from_definition(registered_construct=self.registered_request_construct_obj, scope=self)