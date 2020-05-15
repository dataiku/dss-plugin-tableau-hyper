# This file is the actual code for the custom Python exporter tableau-hyper_upload

from dataiku.exporter import Exporter
from dataiku.exporter import SchemaHelper
import tempfile, os
from tableau_utils import TableauExport
from tableau_utils import get_project_from_name, get_datasource_from_name

import tableauserverclient as TSC

class CustomExporter(Exporter):

    def __init__(self, config, plugin_config):
        self.config = config
        self.plugin_config = plugin_config
        self.username = config.get('username', None)
        self.password = config.get('password','')
        self.server_url = config.get('server_url', None)
        self.project_name_utf8 = config.get('project', 'Default').encode('utf-8')
        self.output_table_utf8 = config.get('output_table', 'DSS_extract').encode('utf-8')
        self.site_id = config.get('site_id', '')
        self.ssl_cert_path = config.get('ssl_cert_path', None)
        
        
        if self.ssl_cert_path:
            if not os.path.isfile(self.ssl_cert_path):
                raise ValueError('SSL certificate file %s does not exist' % self.ssl_cert_path)
            else:
                #default variables handled by python requests to validate cert (used by underlying tableauserverclient)
                os.environ['REQUESTS_CA_BUNDLE'] = self.ssl_cert_path
                os.environ['CURL_CA_BUNDLE'] = self.ssl_cert_path
        
        self.project = None
        self.datasource = None
        
        if not (self.username and self.password and self.server_url):
            print('Connection params: {}'.format(
                {'username:' : self.username,
                'password:' : '#' * len(self.password),
                'server_url:' : self.server_url})
            )
            raise ValueError("username, password and server_url shall not be empty")

    def open(self, schema):
        print('INFO: Given data schema {}'.format(schema))
        tableau_auth = TSC.TableauAuth(self.username, self.password, site_id=self.site_id)
        server = TSC.Server(self.server_url, use_server_version=True)
        print('Using Tableau server version {}'.format(server.version))
        s_info = server.server_info.get()
        print("\nServer info:")
        print("\tProduct version: {0}".format(s_info.product_version))
        print("\tREST API version: {0}".format(s_info.rest_api_version))
        print("\tBuild number: {0}".format(s_info.build_number))
        
        server.auth.sign_in(tableau_auth)

        self.project = get_project_from_name(server, self.project_name_utf8)
        self.datasource = get_datasource_from_name(server, self.output_table_utf8)

        server.auth.sign_out()
        
        # Fairly ugly. We create and delete a temporary file while retaining its name
        with tempfile.NamedTemporaryFile(prefix="output", suffix=".hyper", dir=os.getcwd()) as f:
            self.output_file = f.name
        print("Tmp file: {}".format(self.output_file))
        self.e = TableauExport(self.output_file , schema['columns'])

    def write_row(self, row):
        """
        Handle one row of data to export
        :param row: a tuple with N strings matching the schema passed to open.
        """
        self.e.insert_array_row(row)
        
        
    def close(self):
        """
        Perform any necessary cleanup
        """
        self.e.close()
        tableau_auth = TSC.TableauAuth(self.username, self.password, site_id=self.site_id)
        server = TSC.Server(self.server_url, use_server_version=True)
        
        server.auth.sign_in(tableau_auth)
        
        try:
            if self.datasource:
                server.datasources.publish(self.datasource, self.output_file, 'Overwrite', connection_credentials=None)
            else:
                new_datasource = TSC.DatasourceItem(self.project.id, name=self.output_table_utf8)
                new_datasource = server.datasources.publish(new_datasource, self.output_file, 'CreateNew')
        except:
            raise
        finally:
            server.auth.sign_out()
            os.remove(self.output_file)

