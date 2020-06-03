"""
Exporter to Tableau Server or Tableau Online
"""

import logging
import os

from dataiku.exporter import Exporter
from tableau_table_writer import TableauTableWriter
from tableau_server_utils import get_project_from_name
from tableau_server_utils import get_full_list_of_projects

import tableauserverclient as tsc

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Plugin: Tableau Hyper API | %(levelname)s - %(message)s')


def remove_empty_keys(dictionary):
    """
    Remove empty keys from dictionary
    :param dictionary:
    :return:
    """
    key_to_remove = []
    for key in dictionary:
        if dictionary[key] is None or dictionary[key] == '':
            key_to_remove.append(key)
    for key in key_to_remove:
        del dictionary[key]


def assert_not_none(variable, var_name):
    """
    Return error message if input variable is not defined
    :param variable: value of the variable
    :param var_name: name of the input variable
    :return:
    """
    assert ((variable is not None) and variable != ''), "Parameter {} should be defined".format(var_name)


class TableauHyperExporter(Exporter):
    """
    Exporter to Tableau Server or Tableau Online

    This exporter will first produce a Tableau Hyper file in the home dss directory, and
    then send it to Tableau Server/Online.
    """
    def __init__(self, config, plugin_config):
        """
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """

        self.plugin_config = plugin_config

        # Extract preset configuration from general configuration
        preset_config = config.pop('tableau_server_connection')

        # Sanitize the two configurations
        logger.info("Processing user interface input parameters...")
        remove_empty_keys(preset_config)
        remove_empty_keys(config)

        # Preset configuration will overwrite the manual configuration
        config = {**config, **preset_config}
        self.config = config # final config

        # Retrieve credentials parameters
        username = config.get('username', None)
        assert_not_none(username, 'username')
        password = config.get('password', None)
        assert_not_none(password, 'password')
        server_name = config.get('server_url', None)
        assert_not_none(server_name, 'server_url')
        site_name = config.get('site_id', None) # site name is optional

        logger.info("Detected following user input configuration:\n"
                    "     username: {},\n"
                    "   server_url: {},\n"
                    "    site_name: {}".format(username, server_name, site_name))

        # Handle ssl certificates
        self.ssl_cert_path = config.get('ssl_cert_path', None)

        if self.ssl_cert_path:
            if not os.path.isfile(self.ssl_cert_path):
                raise ValueError('SSL certificate file %s does not exist' % self.ssl_cert_path)
            else:
                # default variables handled by python requests to validate cert (used by underlying tableauserverclient)
                os.environ['REQUESTS_CA_BUNDLE'] = self.ssl_cert_path
                os.environ['CURL_CA_BUNDLE'] = self.ssl_cert_path

        # Retrieve Tableau Hyper and Server/Online locations and table configurations
        self.output_file_name = config.get('output_table', 'my_dss_table')
        self.project_name = config.get('project', 'Default')
        self.schema_name = 'Extract'
        self.table_name = 'Extract'

        logger.info("Detected following Tableau Hyper file configuration:\n"
                    "   output_file_name: {},\n"
                    "       project_name: {},\n"
                    "        schema_name: {},\n"
                    "         table_name: {},\n".format(
            self.output_file_name, self.project_name, self.schema_name, self.table_name))

        # Instantiate Tableau Writer wrapper
        self.writer = TableauTableWriter(schema_name=self.schema_name, table_name=self.table_name)

        # Init output file location
        self.output_file = None

        # Open connection to Tableau Server
        self.tableau_auth = tsc.TableauAuth(username, password, site_id=site_name)
        self.server = tsc.Server(server_name)

        # For debugging purpose only
        with self.server.auth.sign_in(self.tableau_auth):
            project_names = get_full_list_of_projects(self.server)
            logger.info('Got following projects available on server: {}'.format(project_names))

        # Retrieve target project from Tableau Server/Online
        with self.server.auth.sign_in(self.tableau_auth):
            exists, project = get_project_from_name(self.server, self.project_name)
            if not exists:
                raise ValueError('The project {} does not exist on server.'.format(self.project_name))
            self.tableau_datasource = tsc.DatasourceItem(project.id) # get tableau data source

    def open(self, schema):
        """
        :param schema:
        :return:
        """
        logger.info("Call to open method in upload exporter ...")
        self.output_file = self.output_file_name + ".hyper"
        self.writer.schema_converter.set_dss_storage_types(schema)
        self.writer.create_schema(schema, self.output_file)
        logger.info("Define the temporary output file: {}".format(self.output_file))
        return None

    def open_to_file(self, schema, destination_file_path):
        """
        """
        pass

    def write_row(self, row):
        """
        Handle one row of data to export

        :param row: a tuple with N strings matching the schema passed to open
        """
        self.writer.write_row(row)
        self.writer.row_index += 1
        return True

    def close(self):
        """
        Close the connections and publish DataSource to Tableau Server/Online
        If same DataSource exists, it will be overwritten
        """
        self.writer.close()
        with self.server.auth.sign_in(self.tableau_auth):
            self.server.datasources.publish(self.tableau_datasource, self.output_file, 'Overwrite')
        try:
            os.remove(self.output_file)
        except Exception as err:
            logger.warning("Failed to remove the temporary file...")
            raise err
        return True
