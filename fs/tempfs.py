#!/usr/bin/env python

from osfs import OSFS
import tempfile
from shutil import rmtree

class TempFS(OSFS):

    """Create a Filesystem in a tempory directory (with tempfile.mkdtemp),
    and removes it when the TempFS object is cleaned up."""

    def __init__(self):
        self._temp_dir = tempfile.mkdtemp("fstest")
        OSFS.__init__(self, self._temp_dir)

    def _cleanup(self):
        if self._temp_dir:
            rmtree(self._temp_dir)
            self._temp_dir = ""

    def __del__(self):
        self._cleanup()
