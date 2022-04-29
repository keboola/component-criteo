from __future__ import print_function
import criteo_api_marketingsolutions_v2022_04 as cm
from criteo_api_marketingsolutions_v2022_04 import Configuration
from criteo_api_marketingsolutions_v2022_04.api_client import ApiClient
from criteo_api_marketingsolutions_v2022_04.api import analytics_api
from criteo_api_marketingsolutions_v2022_04.model.statistics_report_query_message import StatisticsReportQueryMessage
from criteo_api_marketingsolutions_v2022_04.exceptions import ApiValueError
from datetime import datetime
from typing import List
from criteo_api_marketingsolutions_v2022_04.rest import ApiException

# There is only one accepted GRANT_TYPE
GRANT_TYPE = 'client_credentials'


class CriteoClientException(Exception):
    pass


class CriteoClient:
    def __init__(self, client: ApiClient) -> None:
        self.client = client

    @classmethod
    def login(cls, username: str, password: str):
        configuration = Configuration(username=username, password=password)
        client = cm.ApiClient(configuration)
        return cls(client=client)

    def get_report(self, dimensions: List[str], metrics: List[str], date_from: datetime, date_to: datetime,
                   currency: str) -> str:
        api_instance = analytics_api.AnalyticsApi(self.client)
        try:
            statistics_report_query_message = StatisticsReportQueryMessage(
                dimensions=dimensions,
                metrics=metrics,
                start_date=date_from,
                end_date=date_to,
                currency=currency,
                format="CSV")
        except ApiValueError as api_exc:
            raise CriteoClientException(api_exc) from api_exc

        try:
            api_response = api_instance.get_adset_report(
                statistics_report_query_message=statistics_report_query_message)
            return api_response
        except ApiException as api_exc:
            raise CriteoClientException(api_exc) from api_exc
