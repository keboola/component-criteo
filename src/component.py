import logging
from io import BufferedReader

import dateparser
import requests
import json
from json.decoder import JSONDecodeError
from os import path, mkdir
from criteo import CriteoClient, CriteoClientException
from datetime import datetime, timedelta
from keboola.utils.header_normalizer import get_normalizer, NormalizerStrategy
from keboola.component.base import ComponentBase, UserException
from typing import List, Iterator, Tuple

KEY_CLIENT_ID = '#client_id'
KEY_CLIENT_SECRET = '#client_secret'
KEY_DATE_RANGE = "date_range"
KEY_DATE_TO = "date_to"
KEY_DATE_FROM = "date_from"
KEY_METRICS = "metrics"
KEY_DIMENSIONS = "dimensions"
KEY_OUT_TABLE_NAME = "out_table_name"
KEY_CURRENCY = "currency"

KEY_LOADING_OPTIONS = "loading_options"
KEY_LOADING_OPTIONS_INCREMENTAL = "incremental"
KEY_LOADING_OPTIONS_PKEY = "pkey"

API_ROW_LIMIT = 100000

REQUIRED_PARAMETERS = [KEY_CLIENT_ID, KEY_CLIENT_SECRET, KEY_DATE_RANGE, KEY_OUT_TABLE_NAME, KEY_METRICS,
                       KEY_DIMENSIONS]
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):

    def __init__(self) -> None:
        logging.info("Initializing Component")
        super().__init__(required_parameters=REQUIRED_PARAMETERS,
                         required_image_parameters=REQUIRED_IMAGE_PARS)

    def run(self) -> None:
        logging.info("Running Component")
        params = self.configuration.parameters

        client_id = params.get(KEY_CLIENT_ID)
        client_secret = params.get(KEY_CLIENT_SECRET)
        access_token = self.get_access_token(client_id, client_secret)

        client = CriteoClient.login(access_token)

        loading_options = params.get(KEY_LOADING_OPTIONS)
        incremental = loading_options.get(KEY_LOADING_OPTIONS_INCREMENTAL)
        pkey = loading_options.get(KEY_LOADING_OPTIONS_PKEY, [])

        if incremental and not pkey:
            raise UserException("A primary key must be set for incremental loading")

        metrics = params.get(KEY_METRICS)
        metrics = self.parse_list_from_string(metrics)

        dimensions = params.get(KEY_DIMENSIONS)
        dimensions = self.parse_list_from_string(dimensions)

        currency = params.get(KEY_CURRENCY, "EUR")

        date_from = params.get(KEY_DATE_FROM)
        date_to = params.get(KEY_DATE_TO)
        date_range = params.get(KEY_DATE_RANGE)
        date_from, date_to = self.get_date_range(date_from, date_to, date_range)

        day_delay = self.estimate_day_delay(client, dimensions, metrics, date_to, currency)
        date_ranges = self.split_date_range(date_from, date_to, day_delay)
        out_table_name = params.get(KEY_OUT_TABLE_NAME)

        header_normalizer = get_normalizer(NormalizerStrategy.DEFAULT)
        out_table_name = header_normalizer.normalize_header([out_table_name])[0]
        table = self.create_out_table_definition(name=out_table_name, incremental=incremental, primary_key=pkey,
                                                 is_sliced=True)
        table.delimiter = ";"
        self.create_sliced_directory(table.full_path)

        logging.info(
            f"Fetching report data for dimensions : {dimensions}, metrics : {metrics}, from {date_from} to "
            f"{date_to}, with currency : {currency}")
        fieldnames = self.fetch_data_and_write(client, dimensions, metrics, date_ranges, currency, table.full_path)
        logging.info("Parsing downloaded results")
        header_normalizer = get_normalizer(NormalizerStrategy.DEFAULT)
        table.columns = header_normalizer.normalize_header(fieldnames)
        self.write_tabledef_manifest(table)

    @staticmethod
    def create_sliced_directory(table_path: str):
        logging.info(f"Creating sliced directory at {table_path}")
        if not path.isdir(table_path):
            mkdir(table_path)

    def fetch_data_and_write(self, client: CriteoClient, dimensions: List[str], metrics: List[str],
                             date_ranges: Iterator, currency: str, out_table_path: str) -> List[str]:
        fieldnames = []
        for i, date_range in enumerate(date_ranges):
            slice_path = path.join(out_table_path, str(i))
            logging.info(f"Downloading report chunk from {date_range[0]} to {date_range[1]}")
            response = self._fetch_report(client, dimensions, metrics, date_range[0], date_range[1], currency)
            response_content = response.read()
            response_content = response_content.decode("utf-8")
            logging.info(f"Response content {response_content}")
            last_header_index = response_content.find('\n')
            header_string = response_content[0:last_header_index].strip()
            fieldnames = self.parse_list_from_string(header_string, delimeter=";")
            row_count = 0
            if response_content:
                row_count = response_content.count("\n")
            if row_count >= API_ROW_LIMIT:
                raise UserException("Fetching of data failed, please create a smaller date range for the report")
            with open(slice_path, 'w', encoding='utf-8') as out:
                out.write(response_content[last_header_index + 1:])
        return fieldnames

    def _fetch_report(self, client: CriteoClient, dimensions: List[str], metrics: List[str], date_from: datetime,
                      date_to: datetime, currency: str) -> BufferedReader:
        logging.info(f"Fetching report for dimensions: {dimensions}, metrics: {metrics}, date_from: {date_from}, date_to: {date_to}, currency: {currency}")
        try:
            return client.get_report(dimensions, metrics, date_from, date_to, currency)
        except CriteoClientException as criteo_exc:
            error_text = self.parse_error(criteo_exc)
            raise UserException(error_text) from criteo_exc

    @staticmethod
    def parse_error(exception: CriteoClientException) -> str:
        logging.error(f"Parsing error from exception: {exception}")
        try:
            error = exception.args[0].body
        except AttributeError:
            try:
                error = exception.args[0].args[0]
                return error
            except IndexError as indx_err:
                raise UserException(f"Failed to parse exception {CriteoClientException}") from indx_err

        if isinstance(error, bytes):
            try:
                error = json.loads(error.decode('utf-8'))
            except JSONDecodeError as json_decode_err:
                raise UserException(f"Failed to parse exception {CriteoClientException}") from json_decode_err

        try:
            if "errors" in error and len(error.get("errors", [])) > 0:
                error_text = f"Failed to fetch data : {error.get('errors')[0].get('code')} : " \
                             f"{error.get('errors')[0].get('detail')}\n Whole error : {error}"
                return error_text
        except AttributeError:
            return str(error)

        error_text = f"Failed to fetch data : {error.get('error')} : {error.get('error_description')}\n" \
                     f" Whole error : {error}"
        return error_text

    @staticmethod
    def parse_list_from_string(string_list: str, delimeter: str = ",") -> List[str]:
        logging.info(f"Parsing list from string: {string_list} with delimiter: {delimeter}")
        list_of_strings = string_list.split(delimeter)
        list_of_strings = [word.strip() for word in list_of_strings]
        return list_of_strings

    def get_date_range(self, date_from_str: str, date_to_str: str, date_range: str) -> Tuple[datetime, datetime]:
        logging.info(f"Getting date range from: {date_from_str}, to: {date_to_str}, range: {date_range}")
        if date_range == "Last week (sun-sat)":
            date_from, date_to = self.get_last_week_dates()
        elif date_range == "Last month":
            date_from, date_to = self.get_last_month_dates()
        elif date_range == "Custom":
            try:
                date_from = dateparser.parse(date_from_str).date()
                date_from = datetime.combine(date_from, datetime.min.time())
                date_to = dateparser.parse(date_to_str).date()
                date_to = datetime.combine(date_to, datetime.min.time())
            except AttributeError:
                raise UserException("Invalid custom date, please check documentation for valid inputs")
        else:
            raise UserException(f"Invalid date range type {date_range}. Must be in : \'Last week (sun-sat)\'"
                                f",\'Last month\', \'Custom\'")
        return date_from, date_to

    @staticmethod
    def split_date_range(start_date: datetime, end_date: datetime, day_delay: int) -> Iterator:
        logging.info(f"Splitting date range from: {start_date} to: {end_date} with day delay: {day_delay}")
        delta = timedelta(days=day_delay)
        current_date = start_date
        if current_date + delta < end_date:
            while current_date + delta < end_date:
                todate = current_date + delta
                yield current_date, todate
                current_date += delta + timedelta(days=1)
            yield current_date, end_date
        else:
            yield start_date, end_date

    @staticmethod
    def get_last_week_dates() -> Tuple[datetime, datetime]:
        logging.info("Getting last week dates")
        today = datetime.today()
        offset = (today.weekday() - 5) % 7
        last_week_saturday = today - timedelta(days=offset)
        last_week_sunday = last_week_saturday - timedelta(days=6)
        return last_week_sunday, last_week_saturday

    @staticmethod
    def get_last_month_dates() -> Tuple[datetime, datetime]:
        logging.info("Getting last month dates")
        last_day_of_prev_month = datetime.today().replace(day=1) - timedelta(days=1)
        start_day_of_prev_month = datetime.today().replace(day=1) - timedelta(days=last_day_of_prev_month.day)
        return start_day_of_prev_month, last_day_of_prev_month

    def estimate_day_delay(self, client: CriteoClient, dimensions: List[str], metrics: List[str], date_to: datetime,
                           currency: str) -> int:
        logging.info(f"Estimating day delay for dimensions: {dimensions}, metrics: {metrics}, date_to: {date_to}, currency: {currency}")
        date_to = date_to - timedelta(days=1)
        date_from = date_to - timedelta(days=30)
        rows_per_day = API_ROW_LIMIT
        response = self._fetch_report(client, dimensions, metrics, date_from, date_to, currency)
        sample_report = response.read()
        logging.info(f"Sample report content : {sample_report}")
        sample_report = sample_report.decode("utf-8")
        if sample_report:
            sample_report_len = int(sample_report.count("\n"))
            if sample_report_len == 0:
                rows_per_day = 0
            else:
                rows_per_day = int(sample_report_len / 31)

        if rows_per_day > 1:
            report_range = int((API_ROW_LIMIT * 0.25) / rows_per_day)
        else:
            report_range = 10

        report_range = min(100, report_range)
        return report_range

    @staticmethod
    def get_access_token(client_id, client_secret) -> str:
        logging.info("Getting access token")
        url = "https://api.criteo.com/oauth2/token"

        payload = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret
        }
        headers = {
            "accept": "application/json",
            "content-type": "application/x-www-form-urlencoded"
        }

        try:
            response = requests.post(url, data=payload, headers=headers)
            response.raise_for_status()

            data = response.json()

        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response is not None:
                if e.response.status_code == 401:
                    raise UserException("Failed to authenticate using client credentials.")
                else:
                    raise UserException(f"{url} returned an unexpected error during authentication:"
                                        f" {e.response.status_code}")
            raise UserException(f"Failed to connect to {url}: {e}") from e

        return data.get("access_token")


if __name__ == "__main__":
    try:
        logging.info("Starting Component")
        comp = Component()
        comp.run()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
