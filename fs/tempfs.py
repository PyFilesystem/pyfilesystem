#!/usr/bin/env python

from osfs import OSFS
import tempfile
from shutil import rmtree

class TempFS(OSFS):

    """Create a Filesystem in a tempory directory (with tempfile.mkdtemp),
    and removes it when the TempFS object is cleaned up."""

    def __init__(self, identifier=None):
        """Creates a temporary Filesystem

        identifier -- A string that is included in the name of the temporary directory,
        default uses "TempFS"

        """
        self._temp_dir = tempfile.mkdtemp(identifier or "TempFS")
        self._cleaned = False
        OSFS.__init__(self, self._temp_dir)

    def __str__(self):
        return '<TempFS in "%s">' % self._temp_dir

    def _cleanup(self):
        """Called by __del__ to remove the temporary directory. Can be called directly,
        but it is probably not advisable."""

        if not self._cleaned:
            rmtree(self._temp_dir)
            self._cleaned = True

    def __del__(self):
        self._cleanup()

if __name__ == "__main__":

    tfs = TempFS()
    print tfs