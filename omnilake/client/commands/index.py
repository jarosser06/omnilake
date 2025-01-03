import time

from logging import getLogger
from typing import Optional

from omnilake.client.client import OmniLake
from omnilake.client.request_definitions import (
    AddEntry,
    AddSource,
    CreateSourceType,
    CreateArchive,
    VectorArchiveConfiguration,
)

from omnilake.client.commands.base import Command

from omnilake.client.fileutil import collect_files


logger = getLogger(__name__)


class RefreshIndexCommand(Command):
    command_name='index'

    description='Create or update the index'

    def __init__(self, omnilake_app_name: Optional[str] = None, omnilake_deployment_id: Optional[str] = None):
        super().__init__()

        # Initialize the OmniLake client
        self.omnilake = OmniLake(
            app_name=omnilake_app_name,
            deployment_id=omnilake_deployment_id,
        )

    @classmethod
    def configure_parser(cls, parser):
        question_parser = parser.add_parser('index', help='Indexes the source code')

        return question_parser

    def create_archive(self, archive_name: str):
        """
        Create an archive if it doesn't exist
        """
        try:
            archive = CreateArchive(
                archive_id=archive_name,
                configuration=VectorArchiveConfiguration(),
                description=f'Archive for source code directory {archive_name}',
            )

            self.omnilake.request(archive)

            time.sleep(30)
        except Exception as e:
            if "Archive already exists" in str(e):
                print('Archive already exists')

            else:
                raise

    def create_source_type(self):
        """
        Create a source type if it doesn't exist
        """
        try:
            source_type = CreateSourceType(
                name='local_file',
                description='A file uploaded from a local system',
                required_fields=['file_name', 'full_file_path', 'file_extension'],
            )

            self.omnilake.request(source_type)

            print('Source type "source_code_file" created')
        except Exception as e:
            if "Source type already exists" in str(e):
                print('Source type "source_code_file" already exists')

            else:
                raise

    def run(self, args):
        print('Indexing...')

        starting_dir = args.base_dir

        logger.debug(f'Indexing base directory: {starting_dir}')

        archive_name = starting_dir.split('/')[-1]

        # Create the archive if it doesn't exist
        # archive should enforce latest version
        self.create_archive(archive_name)

        # Create the source type if it doesn't exist
        self.create_source_type()

        # Iterate over the files in the base directory and load them into the archive
        collected_files = collect_files(starting_dir, ignore_patterns=['.git*', '*__pycache__*', '*.pyc', 'poetry.lock', 'cdk.out*', '.DS_Store'])

        for collected_file in collected_files:
            relative_to_base = str(collected_file.relative_to(starting_dir))

            file_contents = collected_file.read_bytes()

            source = AddSource(
                source_type='local_file',
                source_arguments={
                    'file_name': collected_file.name,
                    'file_extension': collected_file.name.split('.')[-1],
                    'full_file_path': relative_to_base,
                },
            )

            source_result = self.omnilake.request(source)

            source_rn = source_result.response_body['resource_name']

            entry = AddEntry(
                content=file_contents.decode(),
                sources=[source_rn],
                destination_archive_id=archive_name,
                original_of_source=source_rn,
            )

            self.omnilake.request(entry)

            print(f'Added {relative_to_base}')

        print('Indexing complete')