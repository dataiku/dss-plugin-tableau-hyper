"""
    Make a test of write for the export in the python-lib/data/ folder.
    TODO: Instead of bad names, fill with DSS symbolic type meaning and generate mock data
"""

import datetime
import os
import logging
import pdb
import time

from tableauhyperapi import TableDefinition
from tableauhyperapi import HyperProcess
from tableauhyperapi import Telemetry
from tableauhyperapi import Connection
from tableauhyperapi import CreateMode
from tableauhyperapi import Inserter
from tableauhyperapi import SqlType
from tableauhyperapi import TableName

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')

def test_to_hyper_without_geo():
    assert True

def test_to_hyper_with_geo():
    """
    Test the write capability with the Hyper API with geo table.
    :return:
    """
    logger.info("Testing write to hyper file with geo series")

    # Path to the test hyper write file
    path_to_hyper = './data/Test01.hyper'
    if os.path.exists(path_to_hyper):
        os.remove(path_to_hyper)
        logger.info("Removing already existing file {}".format(path_to_hyper))
    logger.info("Writing into file: {}".format(path_to_hyper))

    # Build a schema with full types
    geo_table_name = 'myGeoTable'
    logger.info("Creating the Tableau Hyper Table for geo: {}".format(geo_table_name))
    mock_geo_table = TableDefinition(geo_table_name, [
        TableDefinition.Column('int', SqlType.int()),
        TableDefinition.Column('date', SqlType.date()),
        TableDefinition.Column('bool', SqlType.bool()),
        TableDefinition.Column('text', SqlType.text()),
        TableDefinition.Column('timestamp', SqlType.timestamp()),
        TableDefinition.Column('geography', SqlType.geography()),
    ])

    text_table_name = "myTable"
    logger.info("Creating the Tableau Hyper Table for non geo: {}".format(text_table_name))
    mock_text_table = TableDefinition('myTable', [
        TableDefinition.Column('int', SqlType.int()),
        TableDefinition.Column('date', SqlType.date()),
        TableDefinition.Column('bool', SqlType.bool()),
        TableDefinition.Column('text', SqlType.text()),
        TableDefinition.Column('timestamp', SqlType.timestamp()),
        TableDefinition.Column('geography', SqlType.text()),
    ])

    timestamp = 1545730073
    dt_object = datetime.datetime.fromtimestamp(timestamp)

    mock_row = [2, datetime.date(2019, 6, 13), True, 'Hello', dt_object, "point(-122.338083 47.647528)"]
    logger.info("Trying to write row to hyper: {}".format(mock_row))

    # Write in the data
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        logger.info("The HyperProcess has started.")
        with Connection(hyper.endpoint, path_to_hyper, CreateMode.CREATE_AND_REPLACE) as connection:
            connection.catalog.create_table(mock_geo_table)
            connection.catalog.create_table(mock_text_table)
            connection.catalog.create_schema('mySchema')

            with Inserter(connection, mock_text_table) as inserter:
                inserter.add_row(mock_row)
                inserter.execute()

            rows_count = connection.execute_command(
                command=f"INSERT INTO {mock_geo_table.table_name} SELECT * FROM {mock_text_table.table_name};")

            # Drop the text table. It is no longer needed.
            rows_count = connection.execute_command(
                command=f"DROP TABLE {mock_text_table.table_name};")

            logger.info("The data was added to the table.")
        logger.info("The connection to the Hyper extract file is closed.")
    logger.info("The HyperProcess has shut down.")

    # Read back the data
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        print("The HyperProcess has started.")

        with Connection(hyper.endpoint, path_to_hyper) as connection:
            # TODO: Check the public schema is inconvenient here, maybe check assertion before
            with connection.execute_query(f"SELECT * FROM {TableName('public', 'myGeoTable')}") as result:
                for row in result:
                    print(row)
                print("The connection to the Hyper extract file is closed.")
    print("The HyperProcess has shut down.")

    # TODO: Check with expectations for the incoming row
    return True

if __name__ == "__main__":
    print(test_to_hyper_with_geo())