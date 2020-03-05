"""
    Class dedicated to read the Hyper files and tables.
"""

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')


class TableauTableReader(object):

    def __init__(self):
        """
            Read the Tableau Hyper file and
        """
        pass

    def read_table(self, table_name, schema_name):
        pass

    def create_schema(self):
        """
            Create the dss schema corresponding to the Tableau Hyper Schema.
        :return:
        """
        pass

    def read_row(self, row):
        """

        :param row:
        :return:
        """
        pass

    def update_dss_table(self):
        pass

    def close(self):
        pass