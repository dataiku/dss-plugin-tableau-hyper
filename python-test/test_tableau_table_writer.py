from unittest import TestCase
from pandas import Timestamp
from pandas import NaT
from tableau_table_writer import TableauTableWriter
import logging
import string
import random
import os
from tableauhyperapi import HyperProcess, Connection, Telemetry, TableName, TableDefinition
import pandas as pd
import numpy as np
import tempfile

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Plugin: Testing - Tableau Hyper API | %(levelname)s - %(message)s')


def get_random_alphanumeric_string(length):
    letters_and_digits = string.ascii_letters + string.digits
    result_str = ''.join((random.choice(letters_and_digits) for i in range(length)))
    return result_str


class TableauHyperExporter(object):

    def __init__(self, config, plugin_config):
        """
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        self.config = config
        self.plugin_config = plugin_config
        schema_name = self.config.get("schema_name", "Extract")
        table_name = self.config.get("table_name", "Extract")
        self.writer = TableauTableWriter(schema_name=schema_name, table_name=table_name)
        self.writer.batch_size = 10
        self.output_file = None

    def open_to_file(self, schema, destination_file_path):
        """
        Initial actions for the opening of the output file.

        :param schema: the column names and types of the data
        :param destination_file_path: the path where the exported data should be put
        """
        self.output_file = destination_file_path
        self.writer.schema_converter.set_dss_storage_types(schema)
        self.writer.create_schema(schema, self.output_file)

    def write_row(self, row):
        """
        Handle one row of data to export

        :param row: a tuple with N strings matching the schema passed to open
        """
        self.writer.write_row(row)

    def close(self):
        """
        Called when closing the table.
        """
        self.writer.close()


class TestTableauTableWriter(TestCase):

    def setUp(self):
        self.output_path = tempfile.gettempdir()

    def test_export_null_values(self):
        """
        Test export of DSS table with null values to Hyper file.
        :return:
        """
        nan = float("nan")

        # ===> Define parameters input from DSS for exporter
        config = {}
        plugin_config = {}

        schema = {'columns': [{'name': 'id', 'type': 'bigint'}, {'name': 'name', 'type': 'string'},
                              {'name': 'host_id', 'type': 'int'}, {'name': 'host_id_copy', 'type': 'bigint'},
                              {'name': 'host_id_copy_copy', 'type': 'smallint'},
                              {'name': 'host_name', 'type': 'string'},
                              {'name': 'latitude', 'type': 'double'}, {'name': 'latitude_copy', 'type': 'float'},
                              {'name': 'longitude', 'type': 'double'}, {'name': 'price', 'type': 'double'},
                              {'name': 'price_copy', 'type': 'float'}, {'name': 'minimum_nights', 'type': 'bigint'},
                              {'name': 'number_of_reviews', 'type': 'bigint'},
                              {'name': 'last_review', 'type': 'string'},
                              {'name': 'last_review_parsed', 'type': 'date'},
                              {'name': 'reviews_per_month', 'type': 'double'}], 'userModified': True}

        rows = [(2539.0, 'Clean', 2787.0, 2787.0, '2787', 'John', 40.647, 40.6474, -73.97237, 149.0, 149.0, 1.0, 9.0,
                 '2018-10-19', Timestamp('2018-10-19 00:00:00'), 0.21),
                (2595.0, nan, nan, nan, nan, 'Jennifer', nan, 40.75362014770508, -73.98376999999999, 225.0, 225.0, 1.0,
                 45.0, '2019-05-21', Timestamp('2019-05-21 00:00:00'), 0.38),
                (nan, 'THE VILLAGE OF HARLEM....NEW YORK !', 4632.0, 4632.0, '4632', 'Elisabeth', 40.809020000000004,
                 40.80902099609375, -73.9419, 150.0, nan, 3.0, 0.0, nan, NaT, nan),
                (3831.0, 'Cozy Entire Floor of Brownstone', nan, 4869.0, '4869', nan, 40.685140000000004,
                 40.68513870239258, nan, nan, nan, 1.0, 270.0, '2019-07-05', NaT, 4.64),
                (5022.0, 'Entire Apt: Spacious Studio/Loft by central park', nan, nan, nan, 'Laura', 40.79851,
                 40.79851150512695, -73.94399, 80.0, 80.0, 10.0, 9.0, '2018-11-19', NaT, 0.1)]
        # <===

        # ===> Create a DSS-like exporter
        exporter = TableauHyperExporter(config, plugin_config)
        output_file_name = get_random_alphanumeric_string(10) + '.hyper'
        destination_file_path = os.path.join(self.output_path, output_file_name)
        exporter.open_to_file(schema, destination_file_path)
        for row in rows:
            exporter.write_row(row)
        exporter.close()

        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            with Connection(endpoint=hyper.endpoint, database=destination_file_path) as connection:
                with connection.execute_query(query=f"SELECT * FROM {TableName('Extract', 'Extract')}") as result:
                    rows_from_hyper = list(result)

        assert len(rows) == len(rows_from_hyper)

        count = 0
        valid = 0
        for i in range(len(rows)):
            row_dss, row_hyper = rows[i], rows_from_hyper[i]
            for j in range(len(row_dss)):
                a, b = row_dss[j], row_hyper[j]
                if (pd.isna(a) and pd.isna(b)) or (a == b):
                    valid += 1
                count += 1

        os.remove(destination_file_path)

    def test_export_geo_values(self):
        """
        Test the export of geo values (DSS geopoint storage type)
        :return:
        """
        nan = float("nan")

        # ===> Define parameters input from DSS for exporter
        config = {}
        plugin_config = {}
        schema = {'columns': [{'name': 'id', 'type': 'bigint'}, {'name': 'name', 'type': 'string'},
                              {'name': 'host_id', 'type': 'bigint'}, {'name': 'host_id_copy', 'type': 'bigint'},
                              {'name': 'host_id_copy_copy', 'type': 'bigint'}, {'name': 'host_name', 'type': 'string'},
                              {'name': 'latitude', 'type': 'double'}, {'name': 'latitude_copy', 'type': 'double'},
                              {'name': 'longitude', 'type': 'double'}, {'name': 'price', 'type': 'double'},
                              {'name': 'price_copy', 'type': 'double'}, {'name': 'minimum_nights', 'type': 'bigint'},
                              {'name': 'number_of_reviews', 'type': 'bigint'}, {'name': 'last_review', 'type': 'string'},
                              {'name': 'last_review_parsed', 'type': 'date'}, {'name': 'reviews_per_month', 'type': 'double'},
                              {'name': 'coordinates', 'type': 'geopoint'}], 'userModified': True}
        rows = [
            (2539.0, 'Clean & quiet apt home by the park', 2787.0, 2787.0, 2787.0, 'John', 40.647490000000005, 40.647490000000005, -73.97237, 149.0, 149.0, 1.0, 9.0, '2018-10-19', Timestamp('2018-10-19 00:00:00'), 0.21, 'POINT(-73.97237 40.64749)'),
            (2595.0, nan, nan, nan, nan, 'Jennifer', nan, 40.75362, -73.98376999999999, 225.0, 225.0, 1.0, 45.0, '2019-05-21', Timestamp('2019-05-21 00:00:00'), 0.38, nan),
            (nan, 'THE VILLAGE OF HARLEM....NEW YORK !', 4632.0, 4632.0, 4632.0, 'Elisabeth', 40.809020000000004, 40.809020000000004, -73.9419, 150.0, nan, 3.0, 0.0, nan, NaT, nan, 'POINT(-73.9419 40.80902)')
        ]
        # <===

        # ===> Create a DSS-like exporter
        exporter = TableauHyperExporter(config, plugin_config)
        exporter.writer.batch_size = 1
        output_file_name = get_random_alphanumeric_string(10) + '.hyper'
        destination_file_path = os.path.join(self.output_path, output_file_name)
        exporter.open_to_file(schema, destination_file_path)
        for row in rows:
            exporter.write_row(row)
        exporter.close()

        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            with Connection(endpoint=hyper.endpoint, database=destination_file_path) as connection:
                with connection.execute_query(query=f"SELECT * FROM {TableName('Extract', 'Extract')}") as result:
                    rows_from_hyper = list(result)

        assert len(rows) == len(rows_from_hyper)

        count = 0
        valid = 0
        for i in range(len(rows)):
            row_dss, row_hyper = rows[i], rows_from_hyper[i]
            for j in range(len(row_dss)):
                a, b = row_dss[j], row_hyper[j]
                if (pd.isna(a) and pd.isna(b)) or (a == b):
                    valid += 1
                count += 1

        os.remove(destination_file_path)

    def test_export_date_values(self):
        """
        Test the export of geo values (DSS geopoint storage type)
        :return:
        """
        nan = float("nan")

        # ===> Define parameters input from DSS for exporter
        config = {}
        plugin_config = {}
        schema = {'columns': [{'name': 'price', 'type': 'double'}, {'name': 'last_review_parsed', 'type': 'date'}], 'userModified': True}
        rows = [(149.0, Timestamp('2018-10-19 13:20:50.349000')), (225.0, Timestamp('2019-05-21 00:00:00')), (150.0, NaT), (80.0, NaT)]
        # <===

        # ===> Create a DSS-like exporter
        exporter = TableauHyperExporter(config, plugin_config)
        output_file_name = get_random_alphanumeric_string(10) + '.hyper'
        destination_file_path = os.path.join(self.output_path, output_file_name)
        exporter.open_to_file(schema, destination_file_path)
        for row in rows:
            exporter.write_row(row)
        exporter.close()

        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            with Connection(endpoint=hyper.endpoint, database=destination_file_path) as connection:
                with connection.execute_query(query=f"SELECT * FROM {TableName('Extract', 'Extract')}") as result:
                    rows_from_hyper = list(result)

        assert len(rows) == len(rows_from_hyper)

        count = 0
        valid = 0
        for i in range(len(rows)):
            row_dss, row_hyper = rows[i], rows_from_hyper[i]
            for j in range(len(row_dss)):
                a, b = row_dss[j], row_hyper[j]
                if (pd.isna(a) and pd.isna(b)) or (a == b):
                    valid += 1
                count += 1

        os.remove(destination_file_path)

    def test_export_int_values(self):
        # ===> Define parameters input from DSS for exporter
        config = {}
        plugin_config = {}
        schema = {'columns': [{'name': 'tiny_int', 'type': 'tinyint'}, {'name': 'small_int', 'type': 'smallint'}, {'name': 'big_int', 'type': 'bigint'}],
                  'userModified': True}
        rows = [(12, 370, -9223372036854775808), (2, 1000, 2147483647), (15, 15000, 21474836477777777), (126, 32766, 2147483648), (127, 32767, 2147483648)]
        # <===

        # ===> Create a DSS-like exporter
        exporter = TableauHyperExporter(config, plugin_config)
        output_file_name = get_random_alphanumeric_string(10) + '.hyper'
        destination_file_path = os.path.join(self.output_path, output_file_name)
        exporter.open_to_file(schema, destination_file_path)
        for row in rows:
            exporter.write_row(row)
        exporter.close()

        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            with Connection(endpoint=hyper.endpoint, database=destination_file_path) as connection:
                with connection.execute_query(query=f"SELECT * FROM {TableName('Extract', 'Extract')}") as result:
                    rows_from_hyper = list(result)

        assert len(rows) == len(rows_from_hyper)

        count = 0
        valid = 0
        for i in range(len(rows)):
            row_dss, row_hyper = rows[i], rows_from_hyper[i]
            for j in range(len(row_dss)):
                a, b = row_dss[j], row_hyper[j]
                if (pd.isna(a) and pd.isna(b)) or (a == b):
                    valid += 1
                count += 1

        os.remove(destination_file_path)
