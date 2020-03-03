"""
    Hyper connection wrapper:

    The hyper connexion is an encapsulated open/close process for openning a connection to
    a db file (hyper format). The main parameters are the schema name, file location and the different tables.

    Writing this class we should be extra-careful about the following:
    - Manipulating the schema and table name that are already in Tableau Hyper
    - Having only one connection to a file at a time
    - Handle the write and the read of geo data

"""

import logging
import os

from tableauhyperapi import TableDefinition
from tableauhyperapi import HyperProcess
from tableauhyperapi import Telemetry
from tableauhyperapi import Connection
from tableauhyperapi import CreateMode
from tableauhyperapi import Inserter
from tableauhyperapi import TableName

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')

def retrieve_table_and_schema(hyper_file):
    """
        Retrieve the available hyper tables in a file.
    :param hyper_file:
    :return:
    """
    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(hyper.endpoint, hyper_file) as connection:
            tables = []
            schema_names = connection.catalog.get_schema_names()
            for schema in schema_names:
                for table in connection.catalog.get_table_names(schema):
                    tables.append(table)
    return tables

def read_hyper_table(hyper_file, table_name, schema_name):
    """
    TODO: Secure the read using the notebooks in Python, what case in local hyper
    TODO: Check the read from the Rest API to Server
    :param hyper_file: path to the hyper file
        example: "./data/superstore_sample_denormalized.hyper"
    :return:
    """

    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(hyper.endpoint, hyper_file) as connection:
            schema_names = connection.catalog.get_schema_names()
            try:
                assert schema_name in schema_names
            except:
                print("Schema does not exists.")
                return False
            table_names = connection.catalog.get_table_names(schema_name)
            try:
                assert table_name in table_names
            except:
                print("Table does not exists.")
            with connection.execute_query(f'SELECT * FROM {TableName(schema_name, table_name)}') as result:
                for row in result:
                    print(row)
    return True


def write_geo_table(hyper_file, regular_table, geo_table, schema, data):
    """

        Wrapper for the write of a geo table in Tableau Hyper.

    :param hyper_file:
    :param regular_table:
    :param geo_table:
    :param schema:
    :param data: Rows to be written in the file

    :return:
    """
    logger.info("Writing a geo table.")
    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        logger.info("The HyperProcess has started.")

        logger.info("Connection to file {}".format(hyper_file))
        with Connection(hyper.endpoint, hyper_file, CreateMode.CREATE_IF_NOT_EXISTS) as connection:
            connection.catalog.create_table_if_not_exists(geo_table)
            connection.catalog.create_table(regular_table)
            connection.catalog.create_schema_if_not_exists(schema)

            with Inserter(connection, regular_table) as inserter:
                inserter.add_rows(data)
                inserter.execute()

            rows_count = connection.execute_command(
                command=f"INSERT INTO {geo_table.table_name} SELECT * FROM {regular_table.table_name};")

            # Drop the text table. It is no longer needed.
            rows_count = connection.execute_command(
                command=f"DROP TABLE {regular_table.table_name};")

            logger.info("The data was added to the table.")
        logger.info("The connection to the Hyper extract file is closed.")
    logger.info("The HyperProcess has shut down.")
    return True


def write_regular_table(hyper_file, regular_table, schema, data):
    """

        Wrappper for the write of a standard (non geo) table in Tableau Hyper.

    :param hyper_file:
    :param regular_table: Name of the table (Should not contain geo column)
    :param schema: Name of the schema
    :param data: List of the data in rows
    :return:

    Usage:

    >>> path_to_hyper = './data/test_write_regular_table.hyper'

    >>> if os.path.exists(path_to_hyper):
    >>>     os.remove(path_to_hyper)

    >>> dataset = MockDataset(include_geo=False, rows_number=100)

    >>> schema_converter = SchemaConversionDSSHyper(debug=True)

    >>> regular_schema = dataset.schema
    >>> regular_table = TableDefinition('myRegularTable', schema_converter.convert_schema_dss_to_hyper(regular_schema))

    >>> schema_converter.set_dss_type_array(regular_schema)

    >>> row = schema_converter.enforce_hyper_type_on_row(dataset.data[0])
    >>> write_regular_table(path_to_hyper, regular_table, regular_schema, [row])

    """
    logger.info("Writing a regular hyper table.")
    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        logger.info("The HyperProcess has started.")
        with Connection(hyper.endpoint, hyper_file, CreateMode.CREATE_IF_NOT_EXISTS) as connection:
            connection.catalog.create_table_if_not_exists(regular_table)
            connection.catalog.create_schema_if_not_exists(schema)

            with Inserter(connection, regular_table) as inserter:
                inserter.add_rows(rows=data)
                inserter.execute()
            logger.info("The data was added to the table.")
        logger.info("The connection to the Hyper extract file is closed.")
    logger.info("The HyperProcess has shut down.")