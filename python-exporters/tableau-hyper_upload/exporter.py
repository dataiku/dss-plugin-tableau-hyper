"""
Exporter to Tableau Server or Tableau Online
"""

import logging
import os

from cache_utils import get_cache_location_from_user_config
from dataiku.exporter import Exporter
from tableau_table_writer import TableauTableWriter
from tableau_server_utils import get_project_from_name, get_full_list_of_projects, get_tableau_server_connection
import tempfile
from custom_exceptions import InvalidPluginParameter

import tableauserverclient as tsc

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Plugin: Tableau Hyper API | %(levelname)s - %(message)s')


def remove_empty_keys(dictionary):
    """
    Remove empty keys from dictionary
    :param dictionary:
    """
    key_to_remove = []
    for key in dictionary:
        if dictionary[key] is None or dictionary[key] == '':
            key_to_remove.append(key)
    for key in key_to_remove:
        del dictionary[key]


def check_null_values(variable_value, variable_name):
    """
    Throw exception if input variable is not defined
    :param variable_value: value of the variable
    :param variable_name: name of the input variable
    """
    if (variable_value is None) or (variable_value == ''):
        raise InvalidPluginParameter(variable_name=variable_name, variable_value=variable_value)


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
        Exporter.__init__(self, config, plugin_config)

        # Init output file location
        self.output_file = None
        self.tmp_output_dir = None

        server_name, username, password, site_name, self.ignore_ssl = get_tableau_server_connection(config)
        
        logger.info("Detected following user input configuration:\n"
                    "     username: {},\n"
                    "   server_url: {},\n"
                    "    site_name: {}".format(username, server_name, site_name))

        # Retrieve Tableau Hyper and Server/Online locations and table configurations
        self.output_file_name = config.get('output_table', 'my_dss_table')
        self.project_id = None
        retrieve_project_list = config.get('retrieve_project_list', False)
        if retrieve_project_list:
            self.project_id = config.get('project_id')
        self.project_name = config.get('project', 'default')
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
        # Open connection to Tableau Server
        self.tableau_auth = tsc.TableauAuth(username, password, site_id=site_name)
        self.server = tsc.Server(server_name)
        if self.ignore_ssl:
            self.server.add_http_options({'verify': False})
            logger.info("HTTP client is ignoring SSL check.")

        # Retrieve target project from Tableau Server/Online
        with self.server.auth.sign_in(self.tableau_auth):
            if self.project_id:
                self.tableau_datasource = tsc.DatasourceItem(self.project_id) # Create new datasource
            else:
                exists, project = get_project_from_name(self.server, self.project_name.encode("utf-8"))
                if not exists:
                    project_names = get_full_list_of_projects(self.server)
                    logger.warning("Target project {} seems to be unexisting on server, projects accessible on server are:"
                                "{}".format(self.project_name, project_names))
                    raise ValueError('The project {} does not exist on server.'.format(self.project_name))
                self.tableau_datasource = tsc.DatasourceItem(project.id) # Create new datasource

    def open(self, schema):
        """
        :param schema:
        """
        logger.info("Call to open method in upload exporter ...")
        cache_absolute_path = get_cache_location_from_user_config()
        # Create a random file path for the temporary write
        self.tmp_output_dir = tempfile.TemporaryDirectory(dir=cache_absolute_path)
        self.output_file = os.path.join(self.tmp_output_dir.name, self.output_file_name + ".hyper")
        self.writer.schema_converter.set_dss_storage_types(schema)
        self.writer.create_schema(schema, self.output_file)
        logger.info("Define the temporary output file: {}".format(self.output_file))

    def open_to_file(self, schema, destination_file_path):
        # Leave method empty here
        pass

    def write_row(self, row):
        """
        Handle one row of data to export

        :param row: a tuple with N strings matching the schema passed to open
        """
        self.writer.write_row(row)

    def close(self):
        """
        Close the connections and publish DataSource to Tableau Server/Online
        If same DataSource exists, it will be overwritten
        """
        self.writer.close()
        with self.server.auth.sign_in(self.tableau_auth):
            self.server.datasources.publish(self.tableau_datasource, self.output_file, 'Overwrite')
        try:
            self.tmp_output_dir.cleanup()
        except Exception as err:
            logger.warning("Failed to remove the temporary file...")
            raise err
