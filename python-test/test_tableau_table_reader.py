from unittest import TestCase
from tableau_table_reader import TableauTableReader
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Plugin: Testing - Tableau Hyper API | %(levelname)s - %(message)s')


class TestTableauTableReader(TestCase):

    def setUp(self):

        file_test_abs_path = "./data/revenue_prediction.hyper"
        self.stream = open(file_test_abs_path, "rb")

    def test_import_hyper(self):

        tableau_reader = TableauTableReader(table_name='Extract', schema_name='Extract')
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
