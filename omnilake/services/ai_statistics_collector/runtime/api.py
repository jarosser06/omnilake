'''Lambda module for the AI Statistics Collector service'''
import logging

from datetime import datetime, timedelta, UTC as utc_tz
from typing import Dict, Optional

from da_vinci.core.logging import Logger
from da_vinci.core.global_settings import setting_value
from da_vinci.exception_trap.client import fn_exception_reporter, ExceptionReporter

from da_vinci.core.rest_service_base import (
    Route,
    SimpleRESTServiceBase,
)

from omnilake.services.ai_statistics_collector.tables.ai_statistics.client import (
    AIStatisticsClient,
    InvocationStatistic,
)


_FN_NAME = "omnilake.service.ai_statistics_collector"


class AIStatCollector(SimpleRESTServiceBase):
    def __init__(self):
        """
        Initialize the EventBusWatcher
        """
        self.ai_statistics = AIStatisticsClient()

        super().__init__(
            routes=[
                Route(
                    handler=self.collect,
                    method='POST',
                    path='/',
                )
            ],
            exception_function_name=_FN_NAME,
            exception_reporter=ExceptionReporter(),
        )

    def collect(self, job_type: str, job_id: str, model_id: str, total_input_tokens: int, total_output_tokens: int,
                invocation_id: Optional[str] = None ,model_parameters: Optional[Dict] = None,
                resulting_entry_id: Optional[str] = None):
        """
        Collect a statistic

        Keyword Arguments:
        job_type -- The type of the job.
        job_id -- The ID of the job.
        model_id -- The ID of the model.
        total_input_tokens -- The total input tokens.
        total_output_tokens -- The total output tokens.
        model_parameters -- The model parameters.
        resulting_entry_id -- The resulting entry ID.
        """
        response_retention = setting_value('omnilake::ai_statistics_collector', 'statistic_retention_days')

        statistic = InvocationStatistic(
            job_type=job_type,
            job_id=job_id,
            invocation_id=invocation_id,
            model_id=model_id,
            resulting_entry_id=resulting_entry_id,
            total_input_tokens=total_input_tokens,
            total_output_tokens=total_output_tokens,
            model_parameters=model_parameters,
            time_to_live=datetime.now(tz=utc_tz) + timedelta(days=response_retention),
        )

        self.ai_statistics.put(statistic)

        return self.respond(
            body={'message': 'Statistic collected'},
            status_code=201,
        )


@fn_exception_reporter(function_name=_FN_NAME, logger=Logger(_FN_NAME))
def api(event: Dict, context: Dict):
    """
    API handler for the AI Statistics Collector

    Keyword Arguments:
        event: The event
        context: The context
    """
    logging.debug(f'Event: {event}')

    collector = AIStatCollector()

    return collector.handle(event=event)