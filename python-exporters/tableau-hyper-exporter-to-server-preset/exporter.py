from dataiku.exporter import Exporter
from dataiku.exporter import SchemaHelper

import logging
import os

from tableauhyperapi import TableDefinition
from tableauhyperapi import HyperProcess
from tableauhyperapi import Telemetry
from tableauhyperapi import Connection
from tableauhyperapi import CreateMode
from tableauhyperapi import Inserter
from tableauhyperapi import TableName

import tableauserverclient as TSC

import tempfile

from mock_dss_generator import MockDataset
from export_schema_conversion import SchemaConversionDSSHyper

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')

class TableauHyperExporter(Exporter):
    """
        Class for Tableau Export to a Tableau Server.
    """

    def __init__(self, config, plugin_config):
        """
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        self.config = config
        self.plugin_config = plugin_config
        self.row_index = 0
        self.data = []

        if 'project_name' not in self.config:
            self.config['project_name'] = 'default'
        if 'table_name' not in self.config:
            self.config['table_name'] = 'my_dss_table'
        if 'schema_name' not in self.config:
            self.config['schema_name'] = 'my_dss_schema'

        self.site_name = self.config['site']
        self.tableau_auth = TSC.TableauAuth(self.config['username'],
                                            self.config['password'],
                                            self.site_name)
        self.server_name = self.config['server']
        self.server = TSC.Server(self.server_name)

        # Check existing files and projects
        with self.server.auth.sign_in(self.tableau_auth):
            all_projects, pagination_item = self.server.projects.get()
            logger.info("Found the following projects in Tableau Server:")
            print("There are {} projects on the Tableau Server".format(len(all_projects)))
            print([project.name for project in all_projects])

        project_id = all_projects[-1].id
        logger.info('Target project in Tableau for publishing: {}'.format(all_projects[-1].name))
        self.target_project_id = project_id

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

        with tempfile.NamedTemporaryFile(prefix="output", suffix=".hyper", dir=os.getcwd()) as f:
            self.output_file = f.name

        schema_converter = SchemaConversionDSSHyper()
        is_geo_table = schema_converter.is_geo(self.schema)
        self.mock_geo_table = TableDefinition(TableName('schema', 'myGeoTable'), schema_converter.convert_schema_dss_to_hyper(self.schema))
        if is_geo_table:
            dss_text_schema = schema_converter.dss_schema_geo_to_text(self.schema)
            self.mock_text_table = TableDefinition(TableName('schema', 'myTextTable'), schema_converter.convert_schema_dss_to_hyper(dss_text_schema))
        self.is_geo_table = is_geo_table
        self.schema_converter = schema_converter

    def write_row(self, row):
        """
        Handle one row of data to export
        :param row: a tuple with N strings matching the schema passed to open.
        """
        typed_row = self.schema_converter.enforce_hyper_type_on_row(row)
        if self.row_index % 10 == 0:
            logger.info('Row with dss type: {}'.format(row))
            logger.info('Row enforced type: {}'.format(typed_row))
        self.data.append(typed_row)
        self.row_index += 1

    def close(self):
        """
            Called when closing the table.
        """

        if self.is_geo_table:
            logger.info("Detect a Geo Table")
            with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
                logger.info("The HyperProcess has started.")
                with Connection(hyper.endpoint, self.output_file, CreateMode.CREATE_AND_REPLACE) as connection:
                    connection.catalog.create_table(self.mock_geo_table)
                    connection.catalog.create_table(self.mock_text_table)
                    connection.catalog.create_schema('mySchema')

                    with Inserter(connection, self.mock_text_table) as inserter:
                        inserter.add_rows(self.data)
                        inserter.execute()

                    rows_count = connection.execute_command(
                        command=f"INSERT INTO {self.mock_geo_table.table_name} SELECT * FROM {self.mock_text_table.table_name};")

                    # Drop the text table. It is no longer needed.
                    rows_count = connection.execute_command(
                        command=f"DROP TABLE {self.mock_text_table.table_name};")

                    logger.info("The data was added to the table.")
                logger.info("The connection to the Hyper extract file is closed.")
            logger.info("The HyperProcess has shut down.")
        else:
            logger.info("Detect a non Geo Table")
            with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
                with Connection(hyper.endpoint, self.output_file, CreateMode.CREATE_AND_REPLACE) as connection:
                    connection.catalog.create_table(self.mock_geo_table)
                    connection.catalog.create_schema('mySchema')
                    with Inserter(connection, self.mock_geo_table) as inserter:
                        inserter.add_rows(rows=self.data)
                        inserter.execute()

        # Send the hyper file to server
        logger.info("Create datasource with project id: {}".format(self.target_project_id))
        new_datasource = TSC.DatasourceItem(self.target_project_id)
        with self.server.auth.sign_in(self.tableau_auth):
            self.server.datasources.publish(new_datasource, self.output_file, 'CreateNew')
