import os
from unittest import TestCase
from custom_exceptions import TemporaryFileAlreadyDeleted
from temporary_file import TemporaryFile


class TestTemporaryFile(TestCase):
    def test_create_file_with_name(self):
        a_file_name = "test_file.hyper"
        temp_file = TemporaryFile(file_name=a_file_name)
        print("test_create_file_with_name: {}".format(temp_file.get_file_path()))

        self.assertIn("test_file", temp_file.get_file_path())

    def test_create_file_without_name(self):
        temp_file = TemporaryFile(
            file_name_prefix="tmp_hyper_file_", file_name_suffix=".hyper"
        )
        print("test_create_file_with_name: {}".format(temp_file.get_file_path()))

        self.assertIn("tmp_hyper_file_", temp_file.get_file_path())
        self.assertIn(".hyper", temp_file.get_file_path())

    def test_clean_correctly(self):
        temp_file = TemporaryFile()
        temp_file_path = temp_file.get_file_path()

        # The file should exist
        self.assertTrue(os.path.exists(temp_file_path))

        # And after `clean` it it should have been deleted
        temp_file.clean()
        with self.assertRaises(TemporaryFileAlreadyDeleted):
            temp_file.get_file_path()

        self.assertFalse(os.path.exists(temp_file_path))
