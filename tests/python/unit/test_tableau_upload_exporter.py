import os
from pathlib import Path
import tempfile
from unittest import TestCase
from unittest.mock import patch

import exporter as upload_exporter
from tableauhyperapi import Connection, TableName
from tableau_server_utils import get_hyper_process
from tableau_table_writer import TableauTableWriter


class TestTableauUploadExporter(TestCase):

    def create_exporter(self, plugin_config):
        """Constructs the production uploader with a mocked Hyper writer and Tableau server."""
        config = {"output_table": "same-name", "retrieve_project_list": True, "project_id": "test-project"}
        connection = ("http://tableau.invalid", "test-user", "test-password", "", False, "legacy-login")
        with patch.object(upload_exporter, "get_tableau_server_connection", return_value=connection), \
                patch.object(upload_exporter, "TableauTableWriter"), \
                patch.object(upload_exporter.tsc, "Server"):
            exporter = upload_exporter.TableauHyperExporter(config, plugin_config)
        exporter.writer.create_schema.side_effect = lambda schema, path: Path(path).write_bytes(b"upload contents")
        return exporter

    def test_unset_directory_preserves_default(self):
        """Checks default allocation and existing close-time cleanup for absent, null, and empty settings."""
        for config in (None, {}, {"temporary_directory": None}, {"temporary_directory": ""}):
            with self.subTest(config=config):
                exporter = self.create_exporter(config)
                exporter.open({"columns": []})
                try:
                    directory = exporter.tmp_output_dir.name
                    self.assertEqual(os.path.realpath(os.path.dirname(directory)), os.path.realpath(tempfile.gettempdir()))
                    self.assertTrue(os.path.isfile(exporter.output_file))
                    exporter.close()
                    exporter.server.datasources.publish.assert_called_once_with(
                        exporter.tableau_datasource, exporter.output_file, "Overwrite")
                    # Checks that closing the export removes its unique temporary subdirectory.
                    self.assertFalse(os.path.exists(directory))
                finally:
                    exporter.tmp_output_dir.cleanup()

    def test_configured_directory_isolates_uploads_and_preserves_shared_storage(self):
        """Checks same-name uploads get separate directories and cleanup preserves other uploads and shared storage."""
        with tempfile.TemporaryDirectory() as base_directory:
            config = {"temporary_directory": base_directory}
            first = self.create_exporter(config)
            second = self.create_exporter(config)
            first.open({"columns": []})
            self.addCleanup(first.tmp_output_dir.cleanup)
            second.open({"columns": []})
            self.addCleanup(second.tmp_output_dir.cleanup)
            export_directory = os.path.join(base_directory, "export")
            self.assertEqual(os.path.dirname(first.tmp_output_dir.name), export_directory)
            self.assertEqual(os.path.dirname(second.tmp_output_dir.name), export_directory)
            self.assertNotEqual(first.output_file, second.output_file)

            first.close()
            self.assertFalse(os.path.exists(first.tmp_output_dir.name))
            self.assertTrue(os.path.isfile(second.output_file))
            second.close()
            self.assertFalse(os.path.exists(second.tmp_output_dir.name))
            self.assertTrue(os.path.isdir(base_directory))
            self.assertTrue(os.path.isdir(export_directory))

    def test_invalid_base_directory_fails_before_writing(self):
        """Checks invalid explicit bases fail before creating a Hyper file in any fallback location."""
        with tempfile.TemporaryDirectory() as base_directory:
            file_path = os.path.join(base_directory, "file")
            Path(file_path).touch()
            # Covers a relative path, a missing directory, and an existing regular file.
            for path in ("relative-directory", os.path.join(base_directory, "missing"), file_path):
                exporter = self.create_exporter({"temporary_directory": path})
                with self.subTest(path=path), \
                        self.assertRaisesRegex(ValueError, "existing absolute directory"):
                    exporter.open({"columns": []})
                exporter.writer.create_schema.assert_not_called()

    def test_publishes_complete_hyper_file_from_configured_directory(self):
        """Checks real Hyper rows are flushed before publication and the per-upload directory is then removed."""
        with tempfile.TemporaryDirectory() as base_directory:
            exporter = self.create_exporter({"temporary_directory": base_directory})
            exporter.writer = TableauTableWriter(config={}, schema_name="Extract", table_name="Extract")
            schema = {"columns": [{"name": "id", "type": "int"}, {"name": "label", "type": "string"}]}

            def check_hyper_contents(datasource, path, mode):
                """Reads the completed upload file at the point where the Tableau client would publish it."""
                self.assertEqual(os.path.dirname(os.path.dirname(path)), os.path.join(base_directory, "export"))
                with get_hyper_process() as hyper, Connection(hyper.endpoint, path) as connection:
                    rows = connection.execute_list_query(f"SELECT * FROM {TableName('Extract', 'Extract')}")
                    self.assertEqual(rows, [[42, "test upload"]])

            exporter.server.datasources.publish.side_effect = check_hyper_contents
            exporter.open(schema)
            try:
                exporter.write_row((42, "test upload"))
                exporter.close()
                exporter.server.datasources.publish.assert_called_once()
                self.assertFalse(os.path.exists(exporter.tmp_output_dir.name))
                self.assertTrue(os.path.isdir(os.path.join(base_directory, "export")))
            finally:
                exporter.writer.close()
                exporter.tmp_output_dir.cleanup()

    def test_export_directory_blocked_by_file(self):
        """Checks a regular file at <base>/export prevents writing without replacing that file."""
        with tempfile.TemporaryDirectory() as base_directory:
            blocking_file = Path(base_directory) / "export"
            blocking_file.write_bytes(b"existing contents")
            exporter = self.create_exporter({"temporary_directory": base_directory})
            with self.assertRaisesRegex(ValueError, "Cannot create temporary export directory"):
                exporter.open({"columns": []})
            exporter.writer.create_schema.assert_not_called()
            self.assertEqual(blocking_file.read_bytes(), b"existing contents")
