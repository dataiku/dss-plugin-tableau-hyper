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

        self.plugin_config = plugin_config
        logger.info("Received following overall config:\n{}".format(config))
        # The standard config is overwritten by the preset config
        preset_config = config.pop('tableau_server_connection')

        for key in preset_config:
            if preset_config[key] == None or preset_config[key] == '':
                del preset_config[key]

        for key in config:
            if config[key] == None or config[key] == '':
                del config[key]

        logger.info("Preset config:\n{}".format(preset_config))
        logger.info("V1 config:\n{}".format(config))

        config = {**config, **preset_config}
        self.config = config

        logger.info("Final config: {}".format(self.config))

        username = config.get('username', None)
        password = config.get('password', None)
        server_name = config.get('server_url', None)
        site_name = config.get('site_id', None)

        self.project_name = config.get('project', 'Samples')
        self.schema_name = config.get('schema_name', 'dss_schema')
        self.table_name = config.get('output_table', 'dss_table')
        if "." in self.table_name:
            raise ValueError("The table name parameter is invalid, remove \'.\' in {}".format(self.table_name))

        # Non mandatory parameter
        self.ssl_cert_path = config.get('ssl_cert_path', None)

        self.writer = TableauTableWriter(schema_name=self.schema_name, table_name=self.table_name)
        self.output_file = None

        self.tableau_auth = tsc.TableauAuth(username, password, site_name)
        self.server = tsc.Server(server_name)

        with self.server.auth.sign_in(self.tableau_auth):
            all_projects, pagination = self.server.projects.get()
            projects_name = [project.name for project in all_projects]
        if self.project_name is None:
            raise ValueError
        self.tableau_datasource = None
        for project in all_projects:
            if project.name == self.project_name:
                self.tableau_datasource = tsc.DatasourceItem(project.id)
        if self.tableau_datasource is None:
            raise ValueError("The target project does not exist in Tableau Server")

    def open(self, schema):
        """
        Leave empty
        :param schema:
        :return:
        """
        self.output_file = self.table_name + ".hyper"
        self.writer.schema_converter.set_dss_storage_types(schema)
        self.writer.create_schema(schema, self.output_file)
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
            self.server.datasources.publish(self.tableau_datasource, self.output_file, 'Overwrite')
        return True
