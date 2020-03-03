"""
    Test the dss to hyper conversion
    #1: Generate a mock table
    #2: Convert the schema to the right format
    #3: Upload to a hyper file
"""

import pdb
import pytest

from mock_dss_generator import MockDataset
from export_schema_conversion import SchemaConversionDSSHyper

from tableauhyperapi import TableDefinition

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

# Define logger for info
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')

# Test path file in the test
path_to_hyper = './data/Test02.hyper'

# Generate mock dataset of 100 lines (with by default a geojson)
dss_output = MockDataset(include_geo=True, rows_number=100)
dss_schema = dss_output.schema

# First schema in case there is geo format
schema_converter = SchemaConversionDSSHyper()
is_geo_table = schema_converter.is_geo(dss_schema)
mock_geo_table = TableDefinition('myGeoTable', schema_converter.convert_schema_dss_to_hyper(dss_schema))
row = schema_converter.enforce_hyper_type_on_row(dss_output.data[0])

if is_geo_table:

    dss_text_schema = schema_converter.dss_schema_geo_to_text(dss_schema)
    mock_text_table = TableDefinition('myTextTable', schema_converter.convert_schema_dss_to_hyper(dss_text_schema))

    pdb.set_trace()

    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        logger.info("The HyperProcess has started.")
        with Connection(hyper.endpoint, path_to_hyper, CreateMode.CREATE_AND_REPLACE) as connection:
            connection.catalog.create_table(mock_geo_table)
            connection.catalog.create_table(mock_text_table)
            connection.catalog.create_schema('mySchema')

            with Inserter(connection, mock_text_table) as inserter:
                inserter.add_rows(dss_output.data)
                inserter.execute()

            rows_count = connection.execute_command(
                command=f"INSERT INTO {mock_geo_table.table_name} SELECT * FROM {mock_text_table.table_name};")

            # Drop the text table. It is no longer needed.
            rows_count = connection.execute_command(
                command=f"DROP TABLE {mock_text_table.table_name};")

            logger.info("The data was added to the table.")
        logger.info("The connection to the Hyper extract file is closed.")
    logger.info("The HyperProcess has shut down.")
