from __future__ import print_function
from retry import retry
import criteo_marketing_transition as cm
from criteo_marketing_transition import Configuration

# There is only one accepted GRANT_TYPE
GRANT_TYPE = 'client_credentials'


class CriteoClient:
    def __init__(self, username, password):
        configuration = Configuration(username=username, password=password)
        self.client = cm.ApiClient(configuration)

    @retry(tries=3, delay=3)
    def get_report(self, dimensions, metrics, date_from, date_to, currency="EUR"):
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
