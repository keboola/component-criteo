import csv
import tempfile
import logging
import dateparser
import json
from criteo.client import CriteoClient, ApiDataException
from datetime import date
from datetime import timedelta
from keboola.utils.header_normalizer import get_normalizer, NormalizerStrategy
from criteo_marketing_transition.rest import ApiException

from keboola.component.base import ComponentBase, UserException

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

REQUIRED_PARAMETERS = [KEY_CLIENT_ID, KEY_CLIENT_SECRET, KEY_DATE_RANGE, KEY_OUT_TABLE_NAME, KEY_METRICS,
                       KEY_DIMENSIONS]
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):

    def __init__(self):
        super().__init__(required_parameters=REQUIRED_PARAMETERS,
                         required_image_parameters=REQUIRED_IMAGE_PARS)

    def run(self):
        params = self.configuration.parameters

        client_id = params.get(KEY_CLIENT_ID)
        client_secret = params.get(KEY_CLIENT_SECRET)

        client = CriteoClient(client_id, client_secret)

        date_from = params.get(KEY_DATE_FROM)
        date_to = params.get(KEY_DATE_TO)
        date_range = params.get(KEY_DATE_RANGE)
        date_from, date_to = self.get_date_range(date_from, date_to, date_range)
        date_ranges = self.split_date_range(date_from, date_to)
        out_table_name = params.get(KEY_OUT_TABLE_NAME)

        loading_options = params.get(KEY_LOADING_OPTIONS)
        incremental = loading_options.get(KEY_LOADING_OPTIONS_INCREMENTAL)
        pkey = loading_options.get(KEY_LOADING_OPTIONS_PKEY, [])

        if incremental:
            if not pkey:
                raise UserException("A primary key must be set for incremental loading")

        metrics = params.get(KEY_METRICS)
        metrics = self.parse_list_from_string(metrics)

        dimensions = params.get(KEY_DIMENSIONS)
        dimensions = self.parse_list_from_string(dimensions)

        currency = params.get(KEY_CURRENCY, "EUR")
        logging.info(
            f"Fetching report data for dimensions : {dimensions}, metrics : {metrics}, from {date_from} to "
            f"{date_to}, with currency : {currency}")
        temp_file = self.fetch_data(client, dimensions, metrics, date_ranges, currency)

        header_normalizer = get_normalizer(NormalizerStrategy.DEFAULT)
        out_table_name = header_normalizer.normalize_header([out_table_name])[0]
        table = self.create_out_table_definition(name=out_table_name, incremental=incremental, primary_key=pkey)

        fieldnames = self.write_from_temp_to_table(temp_file.name, table.full_path, ";")
        header_normalizer = get_normalizer(NormalizerStrategy.DEFAULT)
        table.columns = header_normalizer.normalize_header(fieldnames)
        self.write_tabledef_manifest(table)

    def fetch_data(self, client, dimensions, metrics, date_ranges, currency):
        temp = tempfile.NamedTemporaryFile(mode='w+b', suffix='.csv', delete=False)
        for date_range in date_ranges:
            response = self._fetch_report(client, dimensions, metrics, date_range[0], date_range[1], currency)
            logging.info(f"Downloading report chunk from {date_range[0]} to {date_range[1]}")
            with open(temp.name, 'a', encoding='utf-8') as out:
                out.write(response)
        return temp

    @staticmethod
    def _fetch_report(client, dimensions, metrics, date_from, date_to, currency):
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
                else:
                    raise UserException("Invalid dimensions, please recheck your configuration") from api_exception
        except ApiDataException as data_exception:
            raise UserException(f"API exception code {data_exception}")

    @staticmethod
    def write_from_temp_to_table(temp_file_path, table_path, delimiter):
        with open(temp_file_path, mode='r', encoding='utf-8') as in_file:
            reader = csv.DictReader(in_file, delimiter=delimiter)
            fieldnames = reader.fieldnames
            with open(table_path, mode='wt', encoding='utf-8', newline='') as out_file:
                writer = csv.DictWriter(out_file, reader.fieldnames)
                for row in reader:
                    writer.writerow(row)
        return fieldnames

    @staticmethod
    def parse_list_from_string(string_list):
        list = string_list.split(",")
        list = [word.strip() for word in list]
        return list

    def get_date_range(self, date_from, date_to, date_range):
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
    def split_date_range(startdate, enddate, delta=timedelta(days=30)):
        currentdate = startdate
        todate = startdate
        while currentdate + delta < enddate:
            todate = currentdate + delta
            yield str(currentdate), str(todate)
            currentdate += delta + timedelta(days=1)
        yield str(todate), str(enddate)

    @staticmethod
    def get_last_week_dates():
        today = date.today()
        offset = (today.weekday() - 5) % 7
        last_week_saturday = today - timedelta(days=offset)
        last_week_sunday = last_week_saturday - timedelta(days=6)
        return last_week_sunday, last_week_saturday

    def get_last_month_dates(self):
        last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)
        start_day_of_prev_month = date.today().replace(day=1) - timedelta(days=last_day_of_prev_month.day)
        return start_day_of_prev_month, last_day_of_prev_month


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
