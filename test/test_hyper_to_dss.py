"""
    Test of the load of a hyper file table in order to read as CSV for example or other APIs.
"""

import pdb

from tableauhyperapi import HyperProcess
from tableauhyperapi import Telemetry
from tableauhyperapi import Connection
from tableauhyperapi import TableName

path_to_hyper = "./data/superstore_sample_denormalized.hyper"

with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    with Connection(hyper.endpoint, path_to_hyper) as connection:
        available_tables = []
        schema_names = connection.catalog.get_schema_names()
        for schema in schema_names:
            table_names = connection.catalog.get_table_names(schema.name)
            for table in table_names:
                available_tables.append(table)

table = TableName('Extract', 'Extract')

with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    print("The HyperProcess has started.")
    with Connection(hyper.endpoint, path_to_hyper) as connection:
        with connection.execute_query(f'SELECT * FROM {table}') as result:
            for row in result:
                print(row)
            print("The connection to the Hyper extract file is closed.")
print("The HyperProcess has shut down.")

# Read the schema
with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    print("The HyperProcess has started.")
    with Connection(hyper.endpoint, path_to_hyper) as connection:
        var = connection.catalog.get_table_definition(table)
    print("The connection to the Hyper extract file is closed.")
print("The HyperProcess has shut down.")

schema = {}
for column in var.columns:
    schema[str(column.name)] = str(column.type)

print(schema)

assert schema["Category"] == "TEXT"

pdb.set_trace()