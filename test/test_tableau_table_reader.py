"""
Test file for the Tableau Table Reader Object in DSS and more particularly for the
offset and limit.
"""

import logging
import os

from tableau_table_reader import TableauTableReader

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')

def test_fetch_rows():
    tableau_reader = TableauTableReader(
                            table_name='dss_table',
                            schema_name='dss_schema'
                                        )
    path_to_hyper = "data/hyper_test_file.hyper"
    tableau_reader.read_hyper_file(path_to_hyper)
    tableau_reader.open_connection()
    tableau_reader.read_hyper_columns()
    logger.info(tableau_reader.hyper_storage_types)
    logger.info(tableau_reader.read_schema())
    for i in range(10**4+100):
        row = tableau_reader.read_row()
    assert True
