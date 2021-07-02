import csv
import tempfile
import logging
import dateparser
from criteo.client import CriteoClient
from datetime import date
from datetime import timedelta
from keboola.utils.header_normalizer import get_normalizer, NormalizerStrategy

from keboola.component.base import ComponentBase, UserException

KEY_CLIENT_ID = '#client_id'
KEY_CLIENT_SECRET = '#client_secret'
KEY_DATE_RANGE = "date_range"
KEY_DATE_TO = "date_to"
KEY_DATE_FROM = "date_from"
KEY_METRICS = "metrics"
KEY_DIMENSIONS = "dimensions"
KEY_OUT_TABLE_NAME = "out_table_name"

KEY_LOADING_OPTIONS = "loading_options"
KEY_LOADING_OPTIONS_INCREMENTAL = "incremental"
KEY_LOADING_OPTIONS_PKEY = "pkey"

REQUIRED_PARAMETERS = []
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

        out_table_name = params.get(KEY_OUT_TABLE_NAME)

        metrics = params.get(KEY_METRICS)
        metrics = self.parse_list_from_string(metrics)

        dimensions = params.get(KEY_DIMENSIONS)
        dimensions = self.parse_list_from_string(dimensions)
        self.get_data_and_write_to_file(client, dimensions, metrics, date_from, date_to, out_table_name)

    def get_data_and_write_to_file(self, client, dimensions, metrics, date_from, date_to, out_table_name):

        response = client.get_report(dimensions, metrics, date_from, date_to)

        temp = tempfile.NamedTemporaryFile(mode='w+b', suffix='.csv', delete=False)
        with open(temp.name, 'w', encoding='utf-8') as out:
            out.write(response)

        table = self.create_out_table_definition(name=out_table_name)
        fieldnames = self.write_from_temp_to_table(temp.name, table.full_path, ";")
        header_normalizer = get_normalizer(NormalizerStrategy.DEFAULT)
        table.columns = header_normalizer.normalize_header(fieldnames)
        self.write_tabledef_manifest(table)

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
            date_from = dateparser.parse(date_from).date()
            date_to = dateparser.parse(date_to).date()
        return date_from, date_to

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
