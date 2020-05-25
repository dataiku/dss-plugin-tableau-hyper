"""
Export to Tableau Server
Allow credentials from plugin preset
"""

import logging
import os

from dataiku.exporter import Exporter
from tableau_table_writer import TableauTableWriter
from tableau_server_utils import get_project_from_name
from tableau_server_utils import get_full_list_of_projects

import tableauserverclient as tsc

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Hyper Plugin | %(levelname)s - %(message)s')


def remove_empty_keys(dictionary):
    key_to_remove = []
    for key in dictionary:
        if dictionary[key] is None or dictionary[key] == '':
            key_to_remove.append(key)
    for key in key_to_remove:
        del dictionary[key]


def assert_not_none(variable, var_name):
    assert ((variable is not None) and variable != ''), "Parameter {} should be defined".format(var_name)


class TableauHyperExporter(Exporter):
    """
    Exporter to Tableau Hyper to Server
    """
    def __init__(self, config, plugin_config):
        """
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """

        self.plugin_config = plugin_config

        # The user config is overwritten by the preset config
        preset_config = config.pop('tableau_server_connection')

        logger.info("Processing user interface input parameters...")
        remove_empty_keys(preset_config)
        remove_empty_keys(config)

        config = {**config, **preset_config} # preset config overwrites user config
        self.config = config # final config

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

        self.ssl_cert_path = config.get('ssl_cert_path', None)

        if self.ssl_cert_path:
            if not os.path.isfile(self.ssl_cert_path):
                raise ValueError('SSL certificate file %s does not exist' % self.ssl_cert_path)
            else:
                # default variables handled by python requests to validate cert (used by underlying tableauserverclient)
                os.environ['REQUESTS_CA_BUNDLE'] = self.ssl_cert_path
                os.environ['CURL_CA_BUNDLE'] = self.ssl_cert_path

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

        # Non mandatory parameter
        self.ssl_cert_path = config.get('ssl_cert_path', None)

        self.writer = TableauTableWriter(schema_name=self.schema_name, table_name=self.table_name)
        self.output_file = None

        self.tableau_auth = tsc.TableauAuth(username, password, site_name)
        self.server = tsc.Server(server_name)

        with self.server.auth.sign_in(self.tableau_auth):
            project_names = get_full_list_of_projects(self.server)
            logger.info('Got following projects available on server: {}'.format(project_names))

        with self.server.auth.sign_in(self.tableau_auth):
            exists, project = get_project_from_name(self.server, self.project_name)
            if not exists:
                raise ValueError('The project {} does not exist on server.'.format(self.project_name))
            self.tableau_datasource = tsc.DatasourceItem(project.id) # get tableau data source

    def open(self, schema):
        """
        Leave empty
        :param schema:
        :return:
        """
        logger.info("Open target storage file in exporter (open method)...")
        self.output_file = self.output_file_name + ".hyper"
        self.writer.schema_converter.set_dss_storage_types(schema)
        self.writer.create_schema(schema, self.output_file)
        return None

    def open_to_file(self, schema, destination_file_path):
        """
        """
        pass

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
        os.remove(self.output_file)
        return True
