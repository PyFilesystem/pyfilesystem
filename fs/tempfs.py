"""
fs.tempfs
=========

Make a temporary file system that exists in a folder provided by the OS. All files contained in a TempFS are removed when the `close` method is called (or when the TempFS is cleaned up by Python).

"""

import os
import os.path
import time
import tempfile

from fs.base import synchronize
from fs.osfs import OSFS
from fs.errors import *

from fs import _thread_synchronize_default


class TempFS(OSFS):

    """Create a Filesystem in a temporary directory (with tempfile.mkdtemp),
    and removes it when the TempFS object is cleaned up."""

    _meta = dict(OSFS._meta)
    _meta['pickle_contents'] = False
    _meta['network'] = False
    _meta['atomic.move'] = True
    _meta['atomic.copy'] = True

    def __init__(self, identifier=None, temp_dir=None, dir_mode=0700, thread_synchronize=_thread_synchronize_default):
        """Creates a temporary Filesystem

        identifier -- A string that is included in the name of the temporary directory,
        default uses "TempFS"

        """
        self.identifier = identifier
        self.temp_dir = temp_dir
        self.dir_mode = dir_mode
        self._temp_dir = tempfile.mkdtemp(identifier or "TempFS", dir=temp_dir)
        self._cleaned = False
        super(TempFS, self).__init__(self._temp_dir, dir_mode=dir_mode, thread_synchronize=thread_synchronize)

    def __repr__(self):
        return '<TempFS: %s>' % self._temp_dir

    __str__ = __repr__

    def __unicode__(self):
        return u'<TempFS: %s>' % self._temp_dir

    def __getstate__(self):
        # If we are picking a TempFS, we want to preserve its contents,
        # so we *don't* do the clean
        state = super(TempFS, self).__getstate__()
        self._cleaned = True
        return state

    def __setstate__(self, state):
        state = super(TempFS, self).__setstate__(state)
        self._cleaned = False
        #self._temp_dir = tempfile.mkdtemp(self.identifier or "TempFS", dir=self.temp_dir)
        #super(TempFS, self).__init__(self._temp_dir,
        #                             dir_mode=self.dir_mode,
        #                             thread_synchronize=self.thread_synchronize)

    @synchronize
    def close(self):
        """Removes the temporary directory.

        This will be called automatically when the object is cleaned up by
        Python, although it is advisable to call it manually.
        Note that once this method has been called, the FS object may
        no longer be used.
        """
        super(TempFS, self).close()
        #  Depending on how resources are freed by the OS, there could
        #  be some transient errors when freeing a TempFS soon after it
        #  was used.  If they occur, do a small sleep and try again.
        try:
            self._close()
        except (ResourceLockedError, ResourceInvalidError):
            time.sleep(0.5)
            self._close()

    @convert_os_errors
    def _close(self):
        """Actual implementation of close().

        This is a separate method so it can be re-tried in the face of
        transient errors.
        """
        os_remove = convert_os_errors(os.remove)
        os_rmdir = convert_os_errors(os.rmdir)
        if not self._cleaned and self.exists("/"):
            self._lock.acquire()
            try:
                # shutil.rmtree doesn't handle long paths on win32,
                # so we walk the tree by hand.
                entries = os.walk(self.root_path, topdown=False)
                for (dir, dirnames, filenames) in entries:
                    for filename in filenames:
                        try:
                            os_remove(os.path.join(dir, filename))
                        except ResourceNotFoundError:
                            pass
                    for dirname in dirnames:
                        try:
                            os_rmdir(os.path.join(dir, dirname))
                        except ResourceNotFoundError:
                            pass
                try:
                    os.rmdir(self.root_path)
                except OSError:
                    pass
                self._cleaned = True
            finally:
                self._lock.release()
        super(TempFS, self).close()
