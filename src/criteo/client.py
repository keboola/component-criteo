from __future__ import print_function
import criteo_api_marketingsolutions_v2023_07 as cm
from criteo_api_marketingsolutions_v2023_07 import Configuration
from criteo_api_marketingsolutions_v2023_07.api_client import ApiClient
from criteo_api_marketingsolutions_v2023_07.api import analytics_api
from criteo_api_marketingsolutions_v2023_07.model.statistics_report_query_message import StatisticsReportQueryMessage
from criteo_api_marketingsolutions_v2023_07.exceptions import ApiValueError
from datetime import datetime
from typing import List
from criteo_api_marketingsolutions_v2023_07.rest import ApiException

# There is only one accepted GRANT_TYPE
GRANT_TYPE = 'client_credentials'


class CriteoClientException(Exception):
    pass


class CriteoClient:
    def __init__(self, client: ApiClient) -> None:
        self.client = client

    @classmethod
    def login(cls, access_token: str):
        configuration = Configuration(access_token=access_token)
        client = cm.ApiClient(configuration)

        return cls(client=client)

    def get_report(self, dimensions: List[str], metrics: List[str], date_from: datetime, date_to: datetime,
                   currency: str, advertiser_ids: str = "") -> str:
        api_instance = analytics_api.AnalyticsApi(self.client)

        try:
            statistics_report_query_message = StatisticsReportQueryMessage(
                dimensions=dimensions,
                metrics=metrics,
                start_date=date_from,
                end_date=date_to,
                currency=currency,
                advertiser_ids=advertiser_ids or "",
                format="CSV")
        except ApiValueError as api_exc:
            raise CriteoClientException(api_exc) from api_exc

        try:
            api_response = api_instance.get_adset_report(
                statistics_report_query_message=statistics_report_query_message)
            return api_response
        except ApiException as api_exc:
            raise CriteoClientException(api_exc) from api_exc
