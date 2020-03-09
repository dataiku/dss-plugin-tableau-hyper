"""
    Test the class test table writer.
"""

from mock_dss_generator import MockDataset
from tableau_table_writer import TableauTableWriter
from tableauhyperapi import HyperProcess, Connection, Telemetry, TableName


def test_tableau_table_writer():
    """
        Test the functionality of the tableau table writer test.
    :return:
    """
    # Use the TableauTableWriter
    path_to_hyper = "./data/test_tableau_table_writer.hyper"
    dataset = MockDataset(include_geo=True, rows_number=0)
    writer = TableauTableWriter(schema_name="transactions", table_name="customers")
    writer.create_schema(dataset.schema, destination_file_path=path_to_hyper)
    writer.write_row(dataset.generate_row())
    writer.update_table()
    writer.close()

    # Read afterward using standard Hyper API
    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        print("The HyperProcess has started.")
        with Connection(hyper.endpoint, path_to_hyper) as connection:
            with connection.execute_query(f"SELECT * FROM {TableName('transactions', 'customers')}") as result:
                for row in result:
                    print(row)
                print("The connection to the Hyper extract file is closed.")
    print("The HyperProcess has shut down.")
    return True


if __name__ == "__main__":
    print(test_tableau_table_writer())