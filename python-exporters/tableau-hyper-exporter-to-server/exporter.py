"""
Export to a Tableau Server. The credentials are stored in the Plugin Preset.
"""

import logging
from dataiku.exporter import Exporter
from tableau_table_writer import TableauTableWriter

import tableauserverclient as tsc

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')


class TableauHyperExporter(Exporter):
    """
    Plugin component (Exporter):
    Export a dataset in DSS to a Hyper file, format of Tableau Software.
    Rely mostly on a wrapper located at ./tableau-hyper-export/python-lib/tableau_table_writer.py
    """

    def __init__(self, config, plugin_config):
        """
        :param config:
        :param plugin_config:
        """
        self.config = config
        self.plugin_config = plugin_config

        self.table_name = self.config.get('table_name')
        if self.table_name is None:
            raise ValueError('The table name parameter is not defined and is mandatory.')

        self.schema_name = self.config.get('schema_name')
        if self.schema_name is None:
            raise ValueError('The schema name parameter is not defined and is mandatory.')

        # Init custom wrapper for writing Tableau hyper file
        self.writer = TableauTableWriter(schema_name=self.schema_name, table_name=self.table_name)

        # To be filled later
        self.output_file = None

        # Get configuration from preset connection or manual definition
        tableau_server_connection = self.config.get('tableau_server_connection')
        server_name = tableau_server_connection['tableau_server']
        site_name = tableau_server_connection['tableau_site']
        username = tableau_server_connection['username']
        password = tableau_server_connection['password']

        self.tableau_auth = tsc.TableauAuth(username, password, site_name)
        self.server = tsc.Server(server_name)

        with self.server.auth.sign_in(self.tableau_auth):
            all_projects, pagination_item = self.server.projects.get()
            projects_name = [project.name for project in all_projects]
            logger.info("Available tables in Tableau Server: {}".format(projects_name))

        self.project_id = self.config.get('project_id')
        if self.project_id is None:
            raise ValueError('The project_id parameter is not defined and is mandatory.')

        available_project_ids = [project.id for project in all_projects]
        if self.project_id not in available_project_ids:
            logger.info("Detected pre-existing project on Tableau Server: {}".format(available_project_ids))
            raise ValueError("The input project_id does not match any existing Tableau project on server.")
        self.tableau_datasource = tsc.DatasourceItem(self.project_id)

    def open(self, schema):
        """
        Leave empty
        :param schema:
        :return:
        """
        return None

    def open_to_file(self, schema, destination_file_path):
        """
       Called when opening the output export file.

       :param schema: dss dataset schema
       :param destination_file_path: path of the export output file
       """
        self.output_file = destination_file_path
        self.writer.schema_converter.set_dss_storage_types(schema)
        self.writer.create_schema(schema, self.output_file)

    def write_row(self, row):
        """
        Receive one row from DSS, iterate on the DSS file.

        :param row: <tuple>
        """
        self.writer.write_row(row)
        self.writer.row_index += 1
        return True

    def close(self):
        """
        Close the connections and send data to Tableau
        """
        self.writer.close()
        # Send the file to Tableau server
        with self.server.auth.sign_in(self.tableau_auth):
            self.server.datasources.publish(self.tableau_datasource, self.output_file, 'CreateNew')
        return True
