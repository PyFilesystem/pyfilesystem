"""

  fs.wrappers:  clases to transform files in a filesystem

This sub-module provides facilities for easily wrapping an FS object inside
some sort of transformation - for example, to transparently compress or encrypt
files.

"""

from fs.base import FS

class FSWrapper(FS):
    """FS that wraps another FS, providing translation etc.

    This class allows simple transforms to be applied to the names
    and/or contents of files in an FS.  It's particularly handy in
    conjunction with wrappers from the "filelike" module.
    """

    def __init__(self,fs):
        super(FSWrapper,self).__init__()
        self.wrapped_fs = fs

    def _file_wrap(self,f,mode):
        """Apply wrapping to an opened file."""
        return f

    def _encode_name(self,name):
        """Encode path component for the underlying FS."""
        return name

    def _decode_name(self,name):
        """Decode path component from the underlying FS."""
        return name

    def _encode(self,path):
        """Encode path for the underlying FS."""
        names = path.split("/")
        e_names = []
        for name in names:
            if name == "":
                e_names.append("")
            else:
                e_names.append(self._encode_name(name))
        return "/".join(e_names)

    def _decode(self,path):
        """Decode path from the underlying FS."""
        names = path.split("/")
        d_names = []
        for name in names:
            if name == "":
                d_names.append("")
            else:
                d_names.append(self._decode_name(name))
        return "/".join(d_names)

    def _adjust_mode(self,mode):
        """Adjust the mode used to open a file in the underlying FS.

        This method takes the mode given when opening a file, and should
        return a two-tuple giving the mode to be used in this FS as first
        item, and the mode to be used in the underlying FS as the second.
        """
        return (mode,mode)

    def getsyspath(self,path,allow_none=False):
        return self.wrapped_fs.getsyspath(self._encode(path),allow_none)

    def hassyspath(self,path):
        return self.wrapped_fs.hassyspath(self._encode(path))

    def open(self,path,mode="r"):
        (mode,wmode) = self._adjust_mode(mode)
        f = self.wrapped_fs.open(self._encode(path),wmode)
        return self._file_wrap(f,mode)

    def exists(self,path):
        return self.wrapped_fs.exists(self._encode(path))

    def isdir(self,path):
        return self.wrapped_fs.isdir(self._encode(path))

    def isfile(self,path):
        return self.wrapped_fs.isfile(self._encode(path))

    def listdir(self,path="",wildcard=None,full=False,absolute=False,dirs_only=False,files_only=False):
        entries = []
        for name in self.wrapped_fs.listdir(self._encode(path),wildcard=None,full=full,absolute=absolute,dirs_only=dirs_only,files_only=files_only):
            entries.append(self._decode(name))
        return self._listdir_helper(path,entries,wildcard=wildcard,full=False,absolute=False,dirs_only=False,files_only=False)

    def makedir(self,path,*args,**kwds):
        return self.wrapped_fs.makedir(self._encode(path),*args,**kwds)

    def remove(self,path):
        return self.wrapped_fs.remove(self._encode(path))

    def removedir(self,path,*args,**kwds):
        return self.wrapped_fs.removedir(self._encode(path),*args,**kwds)

    def rename(self,src,dst):
        return self.wrapped_fs.rename(self._encode(src),self._encode(dst))

    def getinfo(self,path):
        return self.wrapped_fs.getinfo(self._encode(path))

    def desc(self,path):
        return self.wrapped_fs.desc(self._encode(path))

    def copy(self,src,dst,overwrite=False,chunk_size=16384):
        return self.wrapped_fs.copy(self._encode(src),self._encode(dst),overwrite,chunk_size)

    def move(self,src,dst,overwrite=False,chunk_size=16384):
        return self.wrapped_fs.move(self._encode(src),self._encode(dst),overwrite,chunk_size)

    def movedir(self,src,dst,overwrite=False,ignore_errors=False,chunk_size=16384):
        return self.wrapped_fs.movedir(self._encode(src),self._encode(dst),overwrite,ignore_errors,chunk_size)

    def copydir(self,src,dst,overwrite=False,ignore_errors=False,chunk_size=16384):
        return self.wrapped_fs.copydir(self._encode(src),self._encode(dst),overwrite,ignore_errors,chunk_size)

    def __getattr__(self,attr):
        return getattr(self.wrapped_fs,attr)

    def close(self):
        if hasattr(self.wrapped_fs,"close"):
            self.wrapped_fs.close()

