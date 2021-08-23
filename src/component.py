import csv
import tempfile
import logging
import dateparser
import json
from criteo import CriteoClient, ApiDataException
from datetime import date
from datetime import timedelta
from keboola.utils.header_normalizer import get_normalizer, NormalizerStrategy
from criteo_marketing_transition.rest import ApiException
from keboola.component.base import ComponentBase, UserException
from typing import List
from typing import Iterator
from typing import Tuple

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
        super().__init__(required_parameters=REQUIRED_PARAMETERS,
                         required_image_parameters=REQUIRED_IMAGE_PARS)

    def run(self) -> None:
        params = self.configuration.parameters

        client_id = params.get(KEY_CLIENT_ID)
        client_secret = params.get(KEY_CLIENT_SECRET)

        client = CriteoClient.login(client_id, client_secret)

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

        # due to there being a row limit 0f 100k rows, but no automatic pagination; you have to specify a
        # date range which has less than 100k rows. Since the amount of data is not fixed over a period of time
        # you must estimate a safe date range to get data for
        day_delay = self.estimate_day_delay(client, dimensions, metrics, date_to, currency)
        date_ranges = self.split_date_range(date_from, date_to, day_delay)
        out_table_name = params.get(KEY_OUT_TABLE_NAME)

        logging.info(
            f"Fetching report data for dimensions : {dimensions}, metrics : {metrics}, from {date_from} to "
            f"{date_to}, with currency : {currency}")
        temp_file = self.fetch_data(client, dimensions, metrics, date_ranges, currency)
        logging.info("Parsing downloaded results")

        header_normalizer = get_normalizer(NormalizerStrategy.DEFAULT)
        out_table_name = header_normalizer.normalize_header([out_table_name])[0]
        # TODO: use table.delimiter = ";"
        table = self.create_out_table_definition(name=out_table_name, incremental=incremental, primary_key=pkey)

        fieldnames = self.write_from_temp_to_table(temp_file.name, table.full_path, ";")
        header_normalizer = get_normalizer(NormalizerStrategy.DEFAULT)
        table.columns = header_normalizer.normalize_header(fieldnames)
        self.write_tabledef_manifest(table)

    def fetch_data(self, client: CriteoClient, dimensions: List[str], metrics: List[str], date_ranges: Iterator,
                   currency: str) -> tempfile.NamedTemporaryFile:
        # TODO: Just note that if it's multiple files it can be also processed as sliced tables.
        # the storage load can be much faster for large imports if the large file is split into
        # smaller chunks(~<100MB). To do that just store header-less files in a folder
        # e.g. table.full_path = folder.
        # this could also write directly to the final destination out/tables
        temp = tempfile.NamedTemporaryFile(mode='w+b', suffix='.csv', delete=False)
        first_file = True
        for date_range in date_ranges:
            logging.info(f"Downloading report chunk from {date_range[0]} to {date_range[1]}")
            response = self._fetch_report(client, dimensions, metrics, date_range[0], date_range[1], currency)
            if not first_file:
                header_index = response.find('\n')
                response = response[header_index + 1:]
            row_count = response.count("\n")
            if row_count >= API_ROW_LIMIT:
                raise UserException("Fetching of data failed, please create a smaller date range for the report")
            with open(temp.name, 'a', encoding='utf-8') as out:
                out.write(response)
            first_file = False
        return temp

    @staticmethod
    def _fetch_report(client: CriteoClient, dimensions: List[str], metrics: List[str], date_from: date,
                      date_to: date, currency: str) -> str:
        try:
            return client.get_report(dimensions, metrics, date_from, date_to, currency)
        except ApiException as api_exception:
            if "error" in api_exception.body and isinstance(api_exception.body, dict):
                error_type = api_exception.body["error"]
                if error_type == 'credentials_no_longer_supported' or error_type == "invalid_client":
                    raise UserException(
                        "Incorrect credentials, please recheck your authorization settings") from api_exception
            elif "errors" in api_exception.body:
                errors = json.loads(api_exception.body)["errors"]
                if 'detail' in errors[0]:
                    raise UserException(
                        f"Invalid query: {errors[0]['title']},"
                        f" {errors[0]['detail']}") from api_exception
                elif errors[0]["title"] == "Request body is required.":
                    raise UserException(
                        f"List of dimensions in configuration contains an invalid dimension, "
                        f"please recheck your configuration and valid dimensions :"
                        f" https://developers.criteo.com/marketing-solutions/docs/dimensions"
                        f" Your set dimensions : {dimensions}"
                        f"\nError data from Criteo {api_exception.body}") from api_exception
                elif errors[0]["title"] == "At least one advertiser id must be provided.":
                    raise UserException(
                        f"The extractor could not fetch data from the api, please check that your developer app"
                        f" has consented to read data from at least one advertiser"
                        f"\nError data from Criteo {api_exception.body}") from api_exception
        except ApiDataException as data_exception:
            raise UserException(f"API exception code {data_exception}")

    @staticmethod
    def write_from_temp_to_table(temp_file_path: str, table_path: str, delimiter: str) -> List[str]:
        # TODO: this is quite inefficient. There is no need to iterate through the file again. Can be removed.
        # it is already processed in fetch_data method. The delimiter can be set in the manifest
        # out_table_def.delimiter = ';' (it's just missing in the create_out_table method.)
        # The fieldnames can be retrieved in the fetch_data method as well.
        # In case you need to remove first line it can be done like this:
        # https://bitbucket.org/kds_consulting_team/kds-team.ex-shoptet-permalink/src/85dfabb662a47f15fbadcabab893d2ebe0ec9d44/src/component.py#lines-151
        # But here it's not needed because the fetch_data processes data and skips header

        with open(temp_file_path, mode='r', encoding='utf-8') as in_file:
            reader = csv.DictReader(in_file, delimiter=delimiter)
            fieldnames = reader.fieldnames if reader.fieldnames else []
            with open(table_path, mode='wt', encoding='utf-8', newline='') as out_file:
                writer = csv.DictWriter(out_file, reader.fieldnames)
                for row in reader:
                    writer.writerow(row)
        return fieldnames

    @staticmethod
    def parse_list_from_string(string_list: str) -> List[str]:
        # TODO: note that there's keboola.utils.helpers.comma_separated_values_to_list doing the same thing
        list_of_strings = string_list.split(",")
        list_of_strings = [word.strip() for word in list_of_strings]
        return list_of_strings

    def get_date_range(self, date_from: str, date_to: str, date_range: str) -> Tuple[date, date]:
        if date_range == "Last week (sun-sat)":
            date_from, date_to = self.get_last_week_dates()
        elif date_range == "Last month":
            date_from, date_to = self.get_last_month_dates()
        elif date_range == "Custom":
            try:
                date_from = dateparser.parse(date_from).date()
                date_to = dateparser.parse(date_to).date()
            except AttributeError:
                raise UserException("Invalid custom date, please check documentation for valid inputs")
        return date_from, date_to

    @staticmethod
    def split_date_range(start_date: date, end_date: date, day_delay: int) -> Iterator:
        # TODO: just note there's keboola.utils.date.split_dates_to_chunks doing just this
        delta = timedelta(days=day_delay)
        current_date = start_date
        if current_date + delta < end_date:
            while current_date + delta < end_date:
                todate = current_date + delta
                yield str(current_date), str(todate)
                current_date += delta + timedelta(days=1)
            yield str(current_date), str(end_date)
        else:
            yield str(start_date), str(end_date)

    @staticmethod
    def get_last_week_dates():
        today = date.today()
        offset = (today.weekday() - 5) % 7
        last_week_saturday = today - timedelta(days=offset)
        last_week_sunday = last_week_saturday - timedelta(days=6)
        return last_week_sunday, last_week_saturday

    @staticmethod
    def get_last_month_dates():
        last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)
        start_day_of_prev_month = date.today().replace(day=1) - timedelta(days=last_day_of_prev_month.day)
        return start_day_of_prev_month, last_day_of_prev_month

    def estimate_day_delay(self, client: CriteoClient, dimensions: List[str], metrics: List[str], date_to: date,
                           currency: str) -> int:
        """
        Returns the amount of days it is safe to fetch data for
        """
        date_to = date_to - timedelta(days=1)
        date_from = date_to - timedelta(days=30)
        rows_per_day = self._fetch_report(client, dimensions, metrics, date_from, date_to, currency).count("\n") / 31

        # report range is maximum amount of days to get 25% of the api row limit size to be safe as data amount
        # over time can fluctuate
        report_range = int((API_ROW_LIMIT * 0.25) / rows_per_day)
        return report_range


if __name__ == "__main__":
    try:
        comp = Component()
        comp.run()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
