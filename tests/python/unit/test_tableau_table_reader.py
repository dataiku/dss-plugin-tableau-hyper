from unittest import TestCase
from tableau_table_reader import TableauTableReader
import logging
import os
import tempfile

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Plugin: Testing - Tableau Hyper API | %(levelname)s - %(message)s')


class TestTableauTableReader(TestCase):

    def test_configured_directory_creates_unique_import_files(self):
        """Checks that readers sharing one configured base allocate distinct files under import/."""
        with tempfile.TemporaryDirectory() as base_directory:
            readers = [TableauTableReader(config=None, table_name='Extract', schema_name='Extract',
                                         plugin_config={"temporary_directory": base_directory}) for _ in range(2)]
            for reader in readers:
                reader.create_tmp_hyper_file()
                self.assertEqual(os.path.dirname(reader.path_to_hyper), os.path.join(base_directory, "import"))
                self.assertTrue(os.path.isfile(reader.path_to_hyper))
            self.assertNotEqual(readers[0].path_to_hyper, readers[1].path_to_hyper)

    def test_reads_and_cleans_up_file_in_configured_directory(self):
        """Checks a full Hyper import and verifies that cleanup preserves the shared import directory."""
        with tempfile.TemporaryDirectory() as base_directory:
            reader = TableauTableReader(config=None, table_name='Extract', schema_name='Extract',
                                        plugin_config={"temporary_directory": base_directory})
            reader.create_tmp_hyper_file()
            with open("./data/revenue_prediction.hyper", "rb") as stream:
                reader.read_buffer(stream)
            reader.open_connection()
            try:
                reader.read_hyper_columns()
                self.assertTrue(reader.read_schema())
                count = 0
                while reader.read_row() is not None:
                    count += 1
                self.assertEqual(count, 3713)
            finally:
                reader.close_connection()
            self.assertFalse(os.path.exists(reader.path_to_hyper))
            self.assertTrue(os.path.isdir(os.path.join(base_directory, "import")))

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
