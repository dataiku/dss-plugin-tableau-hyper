"""
    Class dedicated to read the Hyper files and tables.
"""

from typing import List

import logging
import os
import tempfile

from tableauhyperapi import TableDefinition
from tableauhyperapi import HyperProcess
from tableauhyperapi import Telemetry
from tableauhyperapi import Connection
from tableauhyperapi import CreateMode
from tableauhyperapi import Inserter
from tableauhyperapi import TableName

from schema_conversion import dss_is_geo
from schema_conversion import geo_to_text
from schema_conversion import SchemaConversion

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')


class TableauTableReader(object):

    def __init__(self):
        """
            Read the Tableau Hyper file and
        """
        self.table_name = None
        self.schema_name = None
        self.path_to_hyper = None
        self.rows = []
        self.hyper = None
        self.connection = None
        self.columns = None
        self.hyper_table = None
        self.row_index = 0
        self.schema_converter = SchemaConversion()

    def create_tmp_hyper(self):
        """
            Create a temporary file for stream buffer storage
        :return: name of the temporary file
        """
        self.path_to_hyper = tempfile.NamedTemporaryFile(prefix='output', suffix=".hyper", dir=os.getcwd()).name
        logger.info("Create a temporary hyper file at location: {}".format(self.path_to_hyper))
        return self.path_to_hyper

    def read_buffer(self, stream):
        """
            Read the full stream for storage and hyper file filling
        :param stream:
        :return:
        """
        lines = stream.readlines()
        for line in lines:
            with open(self.path_to_hyper, "ab") as f:
                f.write(line)
        logger.info("Store the full storage bytes")

    def open_connection(self):
        self.hyper = HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU)
        self.connection = Connection(self.hyper.endpoint, self.path_to_hyper)
        logger.info("Open the connection to Hyper File")

    def check_table_exists(self, table_name, schema_name):
        hyper_tables = []
        schema_names = self.connection.catalog.get_schema_names()
        table_exists = False
        for schema_name_ in schema_names:
            for table_name_ in self.connection.catalog.get_table_names(schema_name_):
                if table_name_.unescaped == table_name and table_name_.schema_name.name.unescaped == schema_name:
                    detected_target_table = True
                hyper_tables.append(table_name_)
        logger.info("Detected the following tables in the hyper files:")
        for hyper_table_ in hyper_tables:
            logger.info("Table: {}".format(hyper_table_))
        return table_exists

    def read_hyper_columns(self):
        hyper_table = TableName(self.schema_name, self.table_name)
        self.hyper_table = hyper_table
        var = self.connection.catalog.get_table_definition(hyper_table)
        columns = [{'name': column.name.unescaped, 'type': str(column.type)} for column in var.columns]
        logger.info("Read schema from the hyper table: {}".format(columns))
        self.columns = columns

    def fetch_rows(self):
        """
            Execute query on the hyper table
        :return:
        """
        result = self.connection.execute_query(f'SELECT * FROM {self.hyper_table}')
        for row in result:
            self.rows.append(row)

    def close_connection(self):
        """
            Close the connection to the Hyper File
        :return:
        """
        self.connection.close()
        self.hyper.close()

    def read_schema(self):
        """
            Wrapper for the format extractor.
        """
        return self.columns

    def read_row(self):
        if self.row_index == len(self.rows):
            return None
        line = self.rows[self.row_index]
        row = {}
        for column, value in zip(self.columns, line):
            row[column['name']] = value
        self.row_index += 1
        return row