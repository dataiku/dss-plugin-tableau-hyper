from dataiku.exporter import Exporter
from dataiku.exporter import SchemaHelper

import joblib
import logging
import os

from tableauhyperapi import TableDefinition
from tableauhyperapi import HyperProcess
from tableauhyperapi import Telemetry
from tableauhyperapi import Connection
from tableauhyperapi import CreateMode
from tableauhyperapi import Inserter

import tableauserverclient as TSC

import tempfile

from mock_dss_generator import MockDataset
from export_schema_conversion import SchemaConversionDSSHyper

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')

class TableauHyperExporter(Exporter):
    """
        DSS exporter class.
        Export a DSS dataset to a Hyper file.
    """

    def __init__(self, config, plugin_config):
        """
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        self.config = config
        self.plugin_config = plugin_config

        # Batch upload and write in Hyper
        self.row_index = 0
        self.data = []
        self.batch_size = +float('inf')

        # Initialised empty values
        self.schema = None
        self.output_file = None
        self.is_geo_table = None
        self.hyper_table = None
        self.regular_hyper_table = None

        # Handle parameters
        if 'table_name' not in self.config:
            self.config['table_name'] = 'my_dss_table'
        if 'schema_name' not in self.config:
            self.config['schema_name'] = 'my_dss_schema'

        # Class components
        self.schema_converter = SchemaConversionDSSHyper()

    def open(self, schema):
        return None

    def open_to_file(self, schema, destination_file_path):
        """
        Start exporting. Only called for exportsers with behavior OUTPUT_TO_FILE
        :param schema: the column names and types of the data that will be streamed
                       in the write_row() calls
        :param destination_file_path: the path where the exported data should be put
        """
        logger.info("Schema of output file:\n {}".format(schema))
        self.schema = schema
        self.schema_converter.set_dss_type_array(self.schema)
        self.output_file = destination_file_path
        self.is_geo_table = self.schema_converter.is_geo(self.schema)

        if self.is_geo_table:
            logger.info("Detected geotable through the schema.")
        else:
            logger.info("Detected regular table through the schema.")

        logger.info("Creating Hyper Table with name: {}".format(self.config['table_name']))
        self.hyper_table = TableDefinition(self.config['table_name'],
                                           self.schema_converter.convert_schema_dss_to_hyper(self.schema))
        if self.is_geo_table:
            dss_regular_schema = self.schema_converter.dss_schema_geo_to_text(self.schema)
            self.regular_hyper_table = TableDefinition(self.config['table_name']+'_geo',
                                                       self.schema_converter.convert_schema_dss_to_hyper(dss_regular_schema))


    def write_row(self, row):
        """
        Handle one row of data to export
        :param row: a tuple with N strings matching the schema passed to open.
        """
        # Process the row
        try:
            typed_row = self.schema_converter.enforce_hyper_type_on_row(row)
            self.data.append(typed_row)
            self.row_index += 1
        except:
            logger.info("Fail to parse the following row: {} \n with following dss schema: {}"
                        .format(row , self.schema_converter.dss_types_array))

    def close(self):
        """
            Called when closing the table.
        """

        if self.is_geo_table:

            logger.info("Two steps write for the geo table")
            with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
                logger.info("The HyperProcess has started.")
                logger.info("Connection to file {}".format(self.output_file))
                with Connection(hyper.endpoint, self.output_file, CreateMode.CREATE_AND_REPLACE) as connection:
                    connection.catalog.create_table(self.hyper_table)
                    connection.catalog.create_table(self.regular_hyper_table)
                    connection.catalog.create_schema(self.config['schema_name'])

                    with Inserter(connection, self.regular_hyper_table) as inserter:
                        inserter.add_rows(self.data)
                        inserter.execute()

                    rows_count = connection.execute_command(
                        command=f"INSERT INTO {self.hyper_table.table_name} SELECT * FROM {self.regular_hyper_table.table_name};")

                    # Drop the text table. It is no longer needed.
                    rows_count = connection.execute_command(
                        command=f"DROP TABLE {self.regular_hyper_table.table_name};")

                    logger.info("The data was added to the table.")
                logger.info("The connection to the Hyper extract file is closed.")
            logger.info("The HyperProcess has shut down.")

        else:

            logger.info("Detect a non Geo Table")

            with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:

                with Connection(hyper.endpoint, self.output_file, CreateMode.CREATE_AND_REPLACE) as connection:
                    connection.catalog.create_table(self.hyper_table)
                    connection.catalog.create_schema('mySchema')

                    with Inserter(connection, self.hyper_table) as inserter:
                        inserter.add_rows(rows=self.data)
                        inserter.execute()
