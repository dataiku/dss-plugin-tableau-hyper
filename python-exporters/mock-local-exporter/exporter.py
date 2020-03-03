# This file is the actual code for the custom Python exporter mock-local-exporter

import joblib
import pdb

from dataiku.exporter import Exporter
from dataiku.exporter import SchemaHelper
import os
import tempfile
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin Dev Status | %(levelname)s - %(message)s')

class MockCSVExporter(Exporter):
    """
        This mock exporter is the standard application for export to csv format but we store the types of DSS when
        exporting or reading the row.
    """

    def __init__(self, config, plugin_config):
        """
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        self.config = config
        self.plugin_config = plugin_config
        self.f = None
        self.schema = None
        self.row_index = 0

    def open(self, schema):
        """
        Start exporting. Only called for exporters with behavior MANAGES_OUTPUT
        :param schema: the column names and types of the data that will be streamed
                       in the write_row() calls
        """
        pass

    def open_to_file(self, schema, destination_file_path):
        """
        Start exporting. Only called for exporters with behavior OUTPUT_TO_FILE
        :param schema: the column names and types of the data that will be streamed
                       in the write_row() calls
        :param destination_file_path: the path where the exported data should be put
        """
        self.schema = schema
        file_path = os.path.join(os.getcwd(), "output.csv")
        print("Fichier temporaire: {}".format(file_path))
        self.f = open(file_path, 'wb')
        self.f.write(b'_index_')
        for column in schema['columns']:
            self.f.write(b'\t')
            self.f.write(column['name'].encode())
        self.f.write(b'\n')

    def write_row(self, row):
        """
        Handle one row of data to export
        :param row: a tuple with N strings matching the schema passed to open.
        """
        if self.row_index == 1:

            var_path = "./plugins/dev/tableau-plugin-dev/python-lib/dss_type_dict"
            type_dict = []
            for (type_, value_) in zip(self.schema['columns'], row):
                type_dict.append((type_['type'], value_))
            joblib.dump(type_dict, var_path)

            var_path = "./plugins/dev/tableau-plugin-dev/python-lib/dss_row"
            joblib.dump(row, var_path)

        self.f.write(str(self.row_index).encode())
        for v in row:
            self.f.write(b'\t')
            self.f.write(('%s' % v).encode() if v is not None else '')
        self.f.write(b'\n')
        self.row_index += 1
        
    def close(self):
        """
        Perform any necessary cleanup
        """
        self.f.close()
