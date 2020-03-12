"""
    Wrapper for the write in the plugin.
"""

import joblib
import logging

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


class TableauTableWriter(object):
    """
        Write a Tableau Hyper File. Use the global conversion functions from DSS to Hyper.
    """

    def __init__(self, schema_name, table_name):
        """
        """
        self.row_index = 0
        self.data = []
        self.batch_size = 2000

        self.schema_name = schema_name
        self.table_name = table_name

        self.output_file = None
        self.is_geo_table = False

        self.schema_converter = SchemaConversion()

        # Tableau Hyper objects
        self.hyper = None
        self.connection = None
        self.tmp_table_definition = None
        self.output_table_definition = None
        self.tmp_table_inserter = None
        self.output_table_inserter = None

    def create_schema(self, schema_dss, destination_file_path):
        """
        TODO: Test when file is already existing, other schema

        Start exporting. Only called for exporters with behavior OUTPUT_TO_FILE
        :param schema_dss:
        :param schema: the column names and types of the data that will be streamed
                       in the write_row() calls
        :param destination_file_path: the path where the exported data should be put
        """
        self.output_file = destination_file_path
        logger.info("Destination file path: {}".format(destination_file_path))
        # Keep the format of DSS in memory
        logger.info("Detected storage types: {}".format(schema_dss))
        dss_storage_types = [column_descriptor['type'] for column_descriptor in schema_dss["columns"]]
        self.schema_converter.set_dss_storage_types(dss_storage_types)
        # Check geo of the dss table
        self.is_geo_table = dss_is_geo(schema_dss)
        logger.info("The input table contains a geo column: {}".format(self.is_geo_table))
        # Create the output Hyper Table (Similar to the schema in dss)
        if not self.schema_name or not self.table_name:
            logger.warning("Did not received the table or schema name.")
        logger.info("Schema name: {} / Table name: {}".format(self.schema_name, self.table_name))
        self.output_table_definition = TableDefinition(
                        TableName(self.schema_name, self.table_name),
                        self.schema_converter.dss_schema_to_hyper_table(schema_dss))
        # Create the sequence of Tableau Hyper objects for connection to the hyper file and db
        self.hyper = HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU)
        self.connection = Connection(self.hyper.endpoint, self.output_file, CreateMode.CREATE_AND_REPLACE)
        self.connection.catalog.create_schema(self.schema_name)
        self.connection.catalog.create_table(self.output_table_definition)

        if self.is_geo_table:
            logger.info("Creating a temporary table for the input table contains a geo column")
            dss_tmp_schema = geo_to_text(schema_dss)
            self.tmp_table_definition = TableDefinition(
                        TableName(self.schema_name, "tmp_"+self.table_name),
                        self.schema_converter.dss_schema_to_hyper_table(dss_tmp_schema))
            logger.info("Created the temporary table hyper definition with schema")
            if self.connection is None:
                logger.info("Database connection is empty. Should be defined.")
            self.connection.catalog.create_table(self.tmp_table_definition)

    def write_row(self, row):
        """
        Handle one row of data to export
        :param row: a tuple with N strings matching the schema passed to open.
        """
        try:
            hyper_compliant_row = self.schema_converter.prepare_row_to_hyper(row)
            self.data.append(hyper_compliant_row)
            self.row_index += 1

            if self.row_index % self.batch_size == 0:
                self.update_table()
                self.data = []
        except:
            logger.warning("Failed to convert the row to make it compliant.")
            logger.warning("Trying to convert a dss row to hyper")
            logger.warning("Failed on row: {}".format(row))
            raise TypeError
        return True

    def update_table(self):
        """
            Update the target Tableau table with the data stack
        :return:
        """
        if self.is_geo_table:

            self.tmp_table_inserter = Inserter(self.connection, self.tmp_table_definition)

            self.tmp_table_inserter.add_rows(self.data)
            self.tmp_table_inserter.execute()
            self.tmp_table_inserter.close()

            rows_count = self.connection.execute_command(
                command=f"INSERT INTO {self.output_table_definition.table_name} SELECT * FROM {self.tmp_table_definition.table_name};")
            rows_count = self.connection.execute_command(
                command=f"DROP TABLE {self.tmp_table_definition.table_name};")
        else:
            self.output_table_inserter = Inserter(self.connection, self.output_table_definition)
            self.output_table_inserter.add_rows(self.data)
            self.output_table_inserter.execute()
            self.output_table_inserter.close()

        return True

    def close(self):
        """
            Called when closing the table.
        """
        if self.data:
            self.update_table()
            self.data = []
        self.hyper.close()
        self.connection.close()
        return True
