import logging
import os
import tempfile
from typing import Optional

from custom_exceptions import TemporaryFileAlreadyDeleted

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="Plugin: Tableau Hyper API | %(levelname)s - %(message)s"
)


class TemporaryFile(object):
    """
    Manages the creation and life cycle of a temporary file.

    A temporary directory and a temporary file in it are created on initialisation.
    Call `clean` to delete the temporary directory and its temporary file.
    """

    def __init__(
        self,
        file_name: Optional[str] = None,
        file_name_prefix: Optional[str] = None,
        file_name_suffix: Optional[str] = None,
    ):
        """
        Creates a temporary file in a temporary directory. If no file_name is given, a random file name is generated.

        The temporary file will live until the TemporaryFile is removed from the context, or `clean` is called.

        :param file_name: optional file name (you should include the extension/suffix if needed).
        :param file_name_prefix: optional prefix to the file name. Used if `file_name` is not set.
        :param file_name_suffix: optional suffix to the file name. Used if `file_name` is not set.
        """
        self._tmp_dir = tempfile.TemporaryDirectory()
        self._tmp_file = None
        self._tmp_file_path = None

        if file_name:
            self._tmp_file_path = os.path.join(self._tmp_dir.name, file_name)
        else:
            self._tmp_file = tempfile.NamedTemporaryFile(
                dir=self._tmp_dir.name, suffix=file_name_suffix, prefix=file_name_prefix
            )
            self._tmp_file_path = self._tmp_file.name

        logger.info("Temporary file created at {}", self._tmp_file_path)

    def get_file_path(self) -> str:
        """
        Gets the absolute path of the temporary file
        """
        if self._tmp_file_path is None:
            raise TemporaryFileAlreadyDeleted()

        return self._tmp_file_path

    def clean(self):
        """
        Deletes the temporary file (and the temporary directory the file was located in)
        """
        if self._tmp_dir is None:
            raise TemporaryFileAlreadyDeleted()

        if self._tmp_file is not None:
            self._tmp_file.close()

        self._tmp_dir.cleanup()
        self._tmp_dir = None
        self._tmp_file_path = None
        self._tmp_file = None
