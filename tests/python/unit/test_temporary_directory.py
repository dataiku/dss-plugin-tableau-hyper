import os
import tempfile
from unittest import TestCase
from unittest.mock import patch

from temporary_directory import get_temporary_directory


class TestTemporaryDirectory(TestCase):

    def test_unset_directory_preserves_default(self):
        """Checks absent, null, and empty settings to preserve compatibility with existing installations."""
        for plugin_config in (None, {}, {"temporary_directory": None}, {"temporary_directory": ""}):
            with self.subTest(plugin_config=plugin_config):
                self.assertIsNone(get_temporary_directory(plugin_config, "import"))

    def test_creates_and_reuses_operation_directory(self):
        """Checks that repeated calls share the operation directory across independent runs."""
        with tempfile.TemporaryDirectory() as base_directory:
            config = {"temporary_directory": base_directory}
            directory = get_temporary_directory(config, "import")
            self.assertEqual(directory, os.path.join(base_directory, "import"))
            self.assertTrue(os.path.isdir(directory))
            self.assertEqual(get_temporary_directory(config, "import"), directory)

    def test_rejects_relative_missing_and_file_paths(self):
        """Checks invalid bases to prevent silently allocating temporary files elsewhere."""
        with tempfile.TemporaryDirectory() as base_directory:
            file_path = os.path.join(base_directory, "file")
            with open(file_path, "w"):
                pass
            # Covers a relative path, a missing directory, and an existing regular file.
            for path in ("relative-directory", os.path.join(base_directory, "missing"), file_path):
                with self.subTest(path=path), \
                        self.assertRaisesRegex(ValueError, "existing absolute directory"):
                    get_temporary_directory({"temporary_directory": path}, "import")

    def test_rejects_operation_directory_blocked_by_file(self):
        """Checks that an existing file at <base>/import prevents creation of the import directory."""
        with tempfile.TemporaryDirectory() as base_directory:
            with open(os.path.join(base_directory, "import"), "w"):
                pass
            with self.assertRaisesRegex(ValueError, "Cannot create temporary import directory"):
                get_temporary_directory({"temporary_directory": base_directory}, "import")

    def test_reports_directory_creation_permission_error(self):
        """Simulates denied access without depending on the test process's filesystem privileges."""
        with tempfile.TemporaryDirectory() as base_directory, \
                patch("temporary_directory.os.makedirs", side_effect=PermissionError("Permission denied")), \
                self.assertRaisesRegex(ValueError, "Permission denied"):
            get_temporary_directory({"temporary_directory": base_directory}, "import")
