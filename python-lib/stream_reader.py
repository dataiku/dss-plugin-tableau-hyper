"""
    Class for reading a stream from a Hyper file in DSS.
    We logged the stream coming from the dss file format, let's try to read it.
"""

import os
import pdb
import base64

from tableauhyperapi import TableDefinition
from tableauhyperapi import HyperProcess
from tableauhyperapi import Telemetry
from tableauhyperapi import Connection
from tableauhyperapi import CreateMode
from tableauhyperapi import Inserter
from tableauhyperapi import TableName


if __name__ == "__main__":

    path_to_hyper = "/Users/thibaultdesfontaines/devenv/dss-home/stream_trace.hyper"
    assert os.path.exists(path_to_hyper)

    with open(path_to_hyper, "rb") as f:
        stream = f.readlines()

    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(hyper.endpoint, path_to_hyper) as connection:
            tables = []
            schema_names = connection.catalog.get_schema_names()
            for schema in schema_names:
                for table in connection.catalog.get_table_names(schema):
                    tables.append(table)

    table_name = tables[0]

    # Read the schema
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        print("The HyperProcess has started.")
        with Connection(hyper.endpoint, path_to_hyper) as connection:
            var = connection.catalog.get_table_definition(table_name)
        print("The connection to the Hyper extract file is closed.")
    print("The HyperProcess has shut down.")

    schema = {}
    for column in var.columns:
        schema[str(column.name)] = str(column.type)

    columns = [column.name.unescaped for column in var.columns]

    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        print("The HyperProcess has started.")
        with Connection(hyper.endpoint, path_to_hyper) as connection:
            with connection.execute_query(f'SELECT * FROM {table_name}') as result:
                for row in result:
                    print(row)
                print("The connection to the Hyper extract file is closed.")
    print("The HyperProcess has shut down.")

    pdb.set_trace()