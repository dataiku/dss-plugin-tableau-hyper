"""
Export to a Tableau Server with credentials in DSS preset plugin form.
"""

import logging

from dataiku.exporter import Exporter
from tableau_table_writer import TableauTableWriter

import tableauserverclient as tsc

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')


class TableauHyperExporter(Exporter):
    """
        Plugin component (Exporter) to export a dataset in dss to a hyper file format. Based on the TableauTableWriter
        wrapper for the read/write to hyper file Tableau APIs.

        Test location:
            - (DSS flow) dku17: Should be tested on different scenarios
            - (Mock execution) local: Can be tested on mock run locally
    """
    def __init__(self, config, plugin_config):
        """
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        self.config = config
        self.plugin_config = plugin_config
        if not 'table_name' in self.config:
            logger.warning("No table_name detected in config.")
        logger.info("Detected schema_name: {}".format(self.config['schema_name']))
        logger.info("Detected table_name: {}".format(self.config['table_name']))
        # Instantiate the Tableau custom writer
        self.writer = TableauTableWriter(schema_name=self.config['schema_name'], table_name=self.config['table_name'])
        self.output_file = None
        # Should contain tableau_server
        logger.info("Preset param: {}".format(self.config))

        server_name = self.config['tableau_server']
        site_name = self.config['tableau_site']
        username = self.config['username']
        password = self.config['password']

        self.tableau_auth = tsc.TableauAuth(username, password, site_name)
        self.server = tsc.Server(server_name)

        with self.server.auth.sign_in(self.tableau_auth):
            all_projects, pagination_item = self.server.projects.get()
            projects_name = [project.name for project in all_projects]
            logger.info("Available tables in Tableau Server: {}".format(projects_name))

        project_id = all_projects[-1].id
        self.tableau_datasource = tsc.DatasourceItem(project_id)

    def open(self, schema):
        """
        Leave empty
        :param schema:
        :return:
        """
        return None

    def open_to_file(self, schema, destination_file_path):
        """
        Initial actions for the opening of the output file.
        :param schema: the column names and types of the data that will be streamed
                       in the write_row() calls
        :param destination_file_path: the path where the exported data should be put
        """
        self.output_file = destination_file_path
        self.writer.schema_converter.set_dss_storage_types(schema)
        self.writer.create_schema(schema, self.output_file)

    def write_row(self, row):
        """
        Handle one row of data to export
        :param row: a tuple with N strings matching the schema passed to open.
        """
        self.writer.write_row(row)
        self.writer.row_index += 1
        return True

    def close(self):
        """
        Close the connections and send data to Tableau
        """
        self.writer.close()
        with self.server.auth.sign_in(self.tableau_auth):
            self.server.datasources.publish(self.tableau_datasource, self.output_file, 'CreateNew')
        return True
