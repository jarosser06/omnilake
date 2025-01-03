'''
List of job types
'''

from enum import StrEnum


class JobType:
    def __init__(self, name: str):
        """
        Job Type

        Keyword arguments:
        name -- Name of the job type
        """
        self.name = name

    def __str__(self):
        return self.name

    def is_job_type(self, job_type: 'JobType') -> bool:
        """
        Check if the job type is the same as the current job type

        Keyword arguments:
        job_type -- Job type to check
        """
        return self.name == job_type.name

    def is_job_type_name(self, job_type_name: str) -> bool:
        """
        Check if the job type is the same as the current job type

        Keyword arguments:
        job_type_name -- Job type name to check
        """
        return self.name.lower() == job_type_name.lower()


class AddEntryJobType(JobType):
    """
    Add Entry Job Type
    """
    def __init__(self):
        super().__init__('ADD_ENTRY')


class CreateArchiveJobType(JobType):
    """
    Create Archive Job Type
    """
    def __init__(self):
        super().__init__('CREATE_ARCHIVE')


class DeleteEntryJobType(JobType):
    """
    Delete Entry Job Type
    """
    def __init__(self):
        super().__init__('DELETE_ENTRY')


class DeleteSourceJobType(JobType):
    """
    Delete Source Job Type
    """
    def __init__(self):
        super().__init__('DELETE_SOURCE')


class JobType(StrEnum):
    """
    Job Type
    """
    ADD_ENTRY = 'ADD_ENTRY'
    CREATE_ARCHIVE = 'CREATE_ARCHIVE'
    DELETE_ENTRY = 'DELETE_ENTRY'
    DELETE_SOURCE = 'DELETE_SOURCE'
    INDEX_ENTRY = 'INDEX_ENTRY'
    INFORMATION_REQUEST = 'INFORMATION_REQUEST'
    PROCESS_ENTRY = 'PROCESS_ENTRY'
    RECALCULATE_VECTOR_TAGS = 'RECALCULATE_VECTOR_TAGS'
    REFRESH_VECTOR_STORES = 'REFRESH_VECTOR_STORES'
    UPDATE_ENTRY = 'UPDATE_ENTRY'

    @staticmethod
    def all():
        return [job_type for job_type in JobType]

    def __str__(self):
        return self.value