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
from tableauhyperapi import HyperException
from tableauhyperapi import TypeTag

from schema_conversion import dss_is_geo
from schema_conversion import geo_to_text
from schema_conversion import SchemaConversion


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')


class TableauTableReader(object):

    def __init__(self, schema_name, table_name):
        """
        Wrapper for the format exporter.
        """
        self.table_name = table_name
        self.schema_name = schema_name
        self.path_to_hyper = None
        self.rows = []
        self.hyper = None
        self.connection = None
        self.columns = None
        self.hyper_table = None
        self.row_index = 0
        self.schema_converter = SchemaConversion()
        self.hyper_storage_types = None
        self.dss_storage_types = None
        self.dss_columns = None

    def create_tmp_hyper(self):
        """
        Create a temporary file for the stream buffer storage

        :return: self.path_to_hyper : The path to the temporary file
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
        """
        Open the connection to the hyper file and the database
        """
        self.hyper = HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU)
        self.connection = Connection(self.hyper.endpoint, self.path_to_hyper)
        logger.info("Open the connection to Hyper File")

    def read_hyper_columns(self):
        """
        Retrieve the target table from the Hyper file.

        :return: self.hyper_storage_types
        """
        logger.info("Trying to read Hyper Table {}.{}".format(self.schema_name, self.table_name))
        hyper_table = TableName(self.schema_name, self.table_name)
        self.hyper_table = hyper_table
        try:
            table_def = self.connection.catalog.get_table_definition(hyper_table)
        except HyperException as e:
            logger.warning("The target table does not exists in this hyper file. Requested table: {}.{}"
                           .format(self.table_name, self.schema_name))
            raise Exception("Table does not exist: {}.{}".format(self.schema_name, self.table_name))

        hyper_columns = [{'name': column.name.unescaped, 'type': column.type.tag} for column in table_def.columns]
        logger.info("Read schema from the hyper table: {}".format(hyper_columns))
        hyper_storage_types = [hyper_column['type'] for hyper_column in hyper_columns]
        dss_storage_types = self.schema_converter.hyper_columns_to_dss_columns(hyper_columns)
        logger.info("Conversion to the following dss storage types: {}".format(dss_storage_types))
        dss_columns = [{'name': column.name.unescaped, 'type': type_} for column, type_ in zip(table_def.columns, dss_storage_types)]
        self.dss_columns = dss_columns
        logger.info("Create the following schema in DSS: {}".format(dss_columns))
        self.schema_converter.set_dss_storage_types(dss_storage_types)
        self.schema_converter.set_hyper_storage_types(hyper_storage_types)
        return hyper_storage_types

    def fetch_rows(self):
        """
        Retrieve all the rows from the Hyper file db
        """
        result = self.connection.execute_query(f'SELECT * FROM {self.hyper_table}')
        for row in result:
            self.schema_converter.prepare_row_to_dss(row)
            self.rows.append(row)

        return True

    def close_connection(self):
        """
            Close the connection to the Hyper File
        :return:
        """
        self.connection.close()
        self.hyper.close()

    def read_schema(self):
        """
        Send the columns for setting the dss dataset schema
        """
        return self.dss_columns

    def read_row(self):
        """
        Read one row from the stored data
        :return:
        """
        if self.row_index == len(self.rows):
            return None
        line = self.rows[self.row_index]
        row = {}
        for column, value in zip(self.dss_storage_types, line):
            row[column['name']] = value
        self.row_index += 1
        return row