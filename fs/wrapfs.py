"""

  fs.wrapfs:  class for wrapping an existing FS object with added functionality

This module provides the class WrapFS, a base class for objects that wrap
another FS object and provide some transformation of its contents.  It could
be very useful for implementing e.g. transparent encryption or compression
services.

As a simple example of how this class could be used, the 'HideDotFiles' class
implements the standard unix shell functionality of hiding dot files in
directory listings.

"""

from fs.base import FS


from fs.errors import wraps, ResourceError

def rewrite_errors(func):
    @wraps(func)
    def wrapper(self,*args,**kwds):
        try:
            return func(self,*args,**kwds)
        except ResourceError, e:
            try:
                e.path = self._decode(e.path)
            except AttributeError:
                pass
            raise
    return wrapper


class WrapFS(FS):
    """FS that wraps another FS, providing translation etc.

    This class allows simple transforms to be applied to the names
    and/or contents of files in an FS.  It could be used to implement
    e.g. compression or encryption in a relatively painless manner.

    The following methods can be overridden to control how files are 
    accessed in the underlying FS object:

        _file_wrap(file,mode):  called for each file that is opened from
                                the underlying FS; may return a modified
                                file-like object.

        _encode(path):  encode a path for access in the underlying FS

        _decode(path):  decode a path from the underlying FS

    If the required path translation proceeds one component at a time,
    it may be simpler to override the _encode_name() and _decode_name()
    methods.
    """

    def __init__(self,fs):
        super(WrapFS,self).__init__()
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

        An example of why this is needed is a WrapFS subclass that does
        transparent file compression - in this case files from the wrapped
        FS cannot be opened in append mode.
        """
        return (mode,mode)

    @rewrite_errors
    def getsyspath(self,path,allow_none=False):
        return self.wrapped_fs.getsyspath(self._encode(path),allow_none)

    @rewrite_errors
    def hassyspath(self,path):
        return self.wrapped_fs.hassyspath(self._encode(path))

    @rewrite_errors
    def open(self,path,mode="r"):
        (mode,wmode) = self._adjust_mode(mode)
        f = self.wrapped_fs.open(self._encode(path),wmode)
        return self._file_wrap(f,mode)

    @rewrite_errors
    def exists(self,path):
        return self.wrapped_fs.exists(self._encode(path))

    @rewrite_errors
    def isdir(self,path):
        return self.wrapped_fs.isdir(self._encode(path))

    @rewrite_errors
    def isfile(self,path):
        return self.wrapped_fs.isfile(self._encode(path))

    @rewrite_errors
    def listdir(self,path="",wildcard=None,full=False,absolute=False,dirs_only=False,files_only=False):
        entries = []
        for name in self.wrapped_fs.listdir(self._encode(path),wildcard=None,full=full,absolute=absolute,dirs_only=dirs_only,files_only=files_only):
            entries.append(self._decode(name))
        return self._listdir_helper(path,entries,wildcard=wildcard,full=False,absolute=False,dirs_only=False,files_only=False)

    @rewrite_errors
    def makedir(self,path,*args,**kwds):
        return self.wrapped_fs.makedir(self._encode(path),*args,**kwds)

    @rewrite_errors
    def remove(self,path):
        return self.wrapped_fs.remove(self._encode(path))

    @rewrite_errors
    def removedir(self,path,*args,**kwds):
        return self.wrapped_fs.removedir(self._encode(path),*args,**kwds)

    @rewrite_errors
    def rename(self,src,dst):
        return self.wrapped_fs.rename(self._encode(src),self._encode(dst))

    @rewrite_errors
    def getinfo(self,path):
        return self.wrapped_fs.getinfo(self._encode(path))

    @rewrite_errors
    def desc(self,path):
        return self.wrapped_fs.desc(self._encode(path))

    @rewrite_errors
    def copy(self,src,dst,overwrite=False,chunk_size=16384):
        return self.wrapped_fs.copy(self._encode(src),self._encode(dst),overwrite,chunk_size)

    @rewrite_errors
    def move(self,src,dst,overwrite=False,chunk_size=16384):
        return self.wrapped_fs.move(self._encode(src),self._encode(dst),overwrite,chunk_size)

    @rewrite_errors
    def movedir(self,src,dst,overwrite=False,ignore_errors=False,chunk_size=16384):
        return self.wrapped_fs.movedir(self._encode(src),self._encode(dst),overwrite,ignore_errors,chunk_size)

    @rewrite_errors
    def copydir(self,src,dst,overwrite=False,ignore_errors=False,chunk_size=16384):
        return self.wrapped_fs.copydir(self._encode(src),self._encode(dst),overwrite,ignore_errors,chunk_size)

    def __getattr__(self,attr):
        return getattr(self.wrapped_fs,attr)

    @rewrite_errors
    def close(self):
        if hasattr(self.wrapped_fs,"close"):
            self.wrapped_fs.close()


class HideDotFiles(WrapFS):
    """FS wrapper class that hides dot-files in directory listings.

    The listdir() function takes an extra keyword argument 'hidden'
    indicating whether hidden dotfiles shoud be included in the output.
    It is False by default.
    """

    def is_hidden(self,path):
        """Check whether the given path should be hidden."""
        return path and basename(path)[0] == "."

    def _encode(self,path):
        return path

    def _decode(self,path):
        return path

    def listdir(self,path="",**kwds):
        hidden = kwds.pop("hidden",True)
        entries = self.wrapped_fs.listdir(path,**kwds)
        if not hidden:
            entries = [e for e in entries if not self.is_hidden(e)]
        return entries

    def walk(self, path="/", wildcard=None, dir_wildcard=None, search="breadth",hidden=False):
        if search == "breadth":
            dirs = [path]
            while dirs:
                current_path = dirs.pop()
                paths = []
                for filename in self.listdir(current_path,hidden=hidden):
                    path = pathjoin(current_path, filename)
                    if self.isdir(path):
                        if dir_wildcard is not None:
                            if fnmatch.fnmatch(path, dir_wilcard):
                                dirs.append(path)
                        else:
                            dirs.append(path)
                    else:
                        if wildcard is not None:
                            if fnmatch.fnmatch(path, wildcard):
                                paths.append(filename)
                        else:
                            paths.append(filename)
                yield (current_path, paths)
        elif search == "depth":
            def recurse(recurse_path):
                for path in self.listdir(recurse_path, wildcard=dir_wildcard, full=True, dirs_only=True,hidden=hidden):
                    for p in recurse(path):
                        yield p
                yield (recurse_path, self.listdir(recurse_path, wildcard=wildcard, files_only=True,hidden=hidden))
            for p in recurse(path):
                yield p
        else:
            raise ValueError("Search should be 'breadth' or 'depth'")


    def isdirempty(self, path):
        path = normpath(path)
        iter_dir = iter(self.listdir(path,hidden=True))
        try:
            iter_dir.next()
        except StopIteration:
            return True
        return False

