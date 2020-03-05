"""
    Test the class test table writer.
"""

from mock_dss_generator import MockDataset
from tableau_table_writer import TableauTableWriter

def test_tableau_table_writer():
    """
        Test the functionality of the tableau table writer test.
    :return:
    """
    path_to_hyper = "./data/test_tableau_table_writer.hyper"
    dataset = MockDataset(include_geo=True, rows_number=0)
    writer = TableauTableWriter(schema_name="transactions", table_name="customers")
    writer.create_schema(dataset.schema, destination_file_path=path_to_hyper)
    writer.write_row(dataset.generate_row())
    writer.update_table()
    writer.close()
    return True


if __name__ == "__main__":
    print(test_tableau_table_writer())