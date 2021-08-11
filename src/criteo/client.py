from __future__ import print_function
import criteo_marketing_transition as cm
from criteo_marketing_transition import Configuration
from criteo_marketing_transition.api_client import ApiClient
from datetime import date
from typing import List

# There is only one accepted GRANT_TYPE
GRANT_TYPE = 'client_credentials'


class ApiDataException(Exception):
    pass


class CriteoClient:
    def __init__(self, client: ApiClient) -> None:
        self.client = client

    @classmethod
    def login(cls, username: str, password: str):
        configuration = Configuration(username=username, password=password)
        client = cm.ApiClient(configuration)
        return cls(client=client)

    def get_report(self, dimensions: List[str], metrics: List[str], date_from: date, date_to: date,
                   currency: str) -> str:
        analytics_api = cm.AnalyticsApi(self.client)
        stats_query_message = cm.StatisticsReportQueryMessage(
            dimensions=dimensions,
            metrics=metrics,
            start_date=date_from,
            end_date=date_to,
            currency=currency,
            format="CSV")

        [response_content, http_code, response_headers] = analytics_api.get_adset_report_with_http_info(
            statistics_report_query_message=stats_query_message)
        if 200 == http_code:
            content_disposition = response_headers["Content-Disposition"]
            if content_disposition:
                return response_content
        else:
            raise ApiDataException(str(http_code))
