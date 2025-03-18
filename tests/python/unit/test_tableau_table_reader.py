from unittest import TestCase
from tableau_table_reader import TableauTableReader
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Plugin: Testing - Tableau Hyper API | %(levelname)s - %(message)s')


class TestTableauTableReader(TestCase):

    def test_read_from_hyper_smaller_than_batch_size(self):
        """
        Reads rows from a file that has less rows than the batch size (10000)  
        """
        file_test_path = "./data/revenue_prediction.hyper"
        self.stream = open(file_test_path, "rb")

        tableau_reader = TableauTableReader(config=None, table_name='Extract', schema_name='Extract')
        tableau_reader.create_tmp_hyper_file()
        tableau_reader.read_buffer(self.stream)
        tableau_reader.open_connection()
        tableau_reader.read_hyper_columns()
        tableau_reader.read_schema()

        count = 0
        row = 1
        while row:
            row = tableau_reader.read_row()
            count += 1
        tableau_reader.close_connection()

        assert (count-1) == 3713

    def test_read_from_hyper_bigger_than_batch_size(self):
        """
        Reads rows from a file that has more rows than the batch size (10000)  

        A second `fetch_rows` will be called.
        """
        file_test_path = "./data/ranked_customers_18766-rows.hyper"
        self.stream = open(file_test_path, "rb")

        tableau_reader = TableauTableReader(config=None, table_name='Extract', schema_name='Extract')
        tableau_reader.create_tmp_hyper_file()
        tableau_reader.read_buffer(self.stream)
        tableau_reader.open_connection()
        tableau_reader.read_hyper_columns()
        tableau_reader.read_schema()

        count = 0
        row = 1
        while row:
            row = tableau_reader.read_row()
            count += 1
        tableau_reader.close_connection()

        assert (count-1) == 18766
