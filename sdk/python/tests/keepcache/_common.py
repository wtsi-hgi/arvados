import os
import shutil
import tempfile
from tempfile import mkdtemp

LOCATOR_1 = "3b83ef96387f14655fc854ddc3c6bd57"
LOCATOR_2 = "73f1eb20517c55bf9493b7dd6e480788"
CACHE_SIZE = 1 * 1024 * 1024 * 16
CONTENTS = bytearray(8 * 1024)
LOCATORS = [LOCATOR_1, LOCATOR_2, "789"]


class TempManager:
    """
    Manages temp files and directories.
    """
    def __init__(self):
        """
        Constructor.
        """
        self.temp_directories = []
        self.temp_files = []

    def create_directory(self):
        """
        Creates a temporary directory.
        :return: the temporary directory
        :rtype: str
        """
        temp_directory = mkdtemp()
        self.temp_directories.append(temp_directory)
        return temp_directory

    def create_file(self):
        """
        Creates a temporary file.
        :return: the path to the temp file
        :rtype: str
        """
        _, temp_file = tempfile.mkstemp()
        self.temp_files.append(temp_file)
        return temp_file

    def remove_all(self):
        """
        Removes all managed temp files and directories.
        """
        self.remove_files()
        self.remove_directories()

    def remove_directories(self):
        """
        Removes all managed temp directories (if they still exist).
        """
        for directory in self.temp_directories:
            if os.path.isdir(directory):
                try:
                    shutil.rmtree(directory)
                except OSError:
                    pass

    def remove_files(self):
        """
        Removes all managed temp files (if they still exist).
        """
        for file in self.temp_files:
            if os.path.exists(file):
                try:
                    os.remove(file)
                except OSError:
                    pass
