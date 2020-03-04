import joblib
import logging
import tempfile
import os

import tableauserverclient as TSC

from export_schema_conversion import SchemaConversionDSSHyper

from hyper_connection_wrapper import write_regular_table
from hyper_connection_wrapper import write_geo_table
from mock_dss_generator import MockDataset

from tableauhyperapi import TableDefinition
from tableauhyperapi import HyperProcess
from tableauhyperapi import Telemetry
from tableauhyperapi import Connection
from tableauhyperapi import CreateMode
from tableauhyperapi import Inserter
from tableauhyperapi import TableName

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')


class TableauTableWriter(object):
    """
        General Tableau Table Writer for reuse in the plugin
    """

    def __init__(self, schema_name, table_name, output_file=None):
        """
        """
        self.row_index = 0
        self.data = []
        self.batch_size = 2000

        self.schema_name = schema_name
        self.table_name = table_name

        self.output_file = output_file
        self.is_geo_table = False

        self.output_table_definition = None
        self.tmp_table_definition = None

        self.schema_converter = SchemaConversionDSSHyper()

    def create_schema(self, schema_dss, destination_file_path):
        """
        TODO: Test when file is already existing, other schema

        Start exporting. Only called for exportsers with behavior OUTPUT_TO_FILE
        :param schema_dss:
        :param schema: the column names and types of the data that will be streamed
                       in the write_row() calls
        :param destination_file_path: the path where the exported data should be put
        """
        self.output_file = destination_file_path
        self.schema_converter.set_dss_type_array(schema_dss)

        self.is_geo_table = self.schema_converter.is_geo(schema_dss)
        self.output_table_definition = TableDefinition(TableName(self.schema_name, self.table_name),
                                           self.schema_converter.convert_schema_dss_to_hyper(schema_dss))

        if self.is_geo_table:
            dss_tmp_schema = self.schema_converter.dss_schema_geo_to_text(schema_dss)
            self.tmp_table_definition = TableDefinition(TableName(self.schema_name, "tmp_"+self.table_name),
                                            self.schema_converter.convert_schema_dss_to_hyper(dss_tmp_schema))

        with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            with Connection(hyper.endpoint, self.output_file, CreateMode.CREATE_AND_REPLACE) as connection:
                connection.catalog.create_schema(self.schema_name)
                connection.catalog.create_table(self.output_table_definition)

    def write_row(self, row):
        """
        Handle one row of data to export
        :param row: a tuple with N strings matching the schema passed to open.
        """
        # Convert values of row to hyper
        typed_row = self.schema_converter.enforce_hyper_type_on_row(row)
        self.data.append(typed_row)
        self.row_index += 1

        if self.row_index % self.batch_size == 0:
            self.update_table()
            self.data = []
        return True

    def update_table(self):
        """
            Update the target Tableau table with the data stack
        :return:
        """
        with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            with Connection(hyper.endpoint, self.output_file, CreateMode.NONE) as connection:

                if self.is_geo_table:

                    connection.catalog.create_table(self.tmp_table_definition)

                    with Inserter(connection, self.tmp_table_definition) as inserter:
                        inserter.add_rows(self.data)
                        inserter.execute()

                    rows_count = connection.execute_command(
                        command=f"INSERT INTO {self.output_table_definition.table_name} SELECT * FROM {self.tmp_table_definition.table_name};")

                    # Drop the text table. It is no longer needed.
                    rows_count = connection.execute_command(
                        command=f"DROP TABLE {self.tmp_table_definition.table_name};")

                else:
                    with Inserter(connection, self.output_table_definition) as inserter:
                        inserter.add_rows(self.data)
                        inserter.execute()

                logger.info("The data was added to the table.")
            logger.info("The connection to the Hyper extract file is closed.")
        logger.info("The HyperProcess has shut down.")
        return True

    def close(self):
        """
            Called when closing the table.
        """
        if self.data:
            self.update_table()
            self.data = []
        return True