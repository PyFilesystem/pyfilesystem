"""
fs.filelike
===========

This module takes care of the groundwork for implementing and manipulating
objects that provide a rich file-like interface, including reading, writing,
seeking and iteration.

The main class is FileLikeBase, which implements the entire file-like interface
on top of primitive _read(), _write(), _seek(), _tell() and _truncate() methods.
Subclasses may implement any or all of these methods to obtain the related
higher-level file behaviors.

Other useful classes include:

    * StringIO:   a version of the builtin StringIO class, patched to more
                  closely preserve the semantics of a standard file.

    * FileWrapper:  a generic base class for wrappers around a filelike object
                    (think e.g. compression or decryption).

    * SpooledTemporaryFile:  a version of the builtin SpooledTemporaryFile
                             class, patched to more closely preserve the
                             semantics of a standard file.

    * LimitBytesFile:  a filelike wrapper that limits the total bytes read
                       from a file; useful for turning a socket into a file
                       without reading past end-of-data.

"""
# Copyright (C) 2006-2009, Ryan Kelly
# All rights reserved; available under the terms of the MIT License.

import tempfile as _tempfile

import fs


class NotReadableError(IOError):
    pass
class NotWritableError(IOError):
    pass
class NotSeekableError(IOError):
    pass
class NotTruncatableError(IOError):
    pass

import six
from six import PY3, b

if PY3:
    from six import BytesIO as _StringIO
else:
    try:
        from cStringIO import StringIO as _StringIO
    except ImportError:
        from StringIO import StringIO as _StringIO


class FileLikeBase(object):
    """Base class for implementing file-like objects.

    This class takes a lot of the legwork out of writing file-like objects
    with a rich interface.  It implements the higher-level file-like methods
    on top of five primitive methods: _read, _write, _seek, _tell and
    _truncate. See their docstrings for precise details on how these methods
    behave.

    Subclasses then need only implement some subset of these methods for
    rich file-like interface compatibility.  They may of course override
    other methods as desired.

    The class is missing the following attributes and methods, which don't
    really make sense for anything but real files:

        * fileno()
        * isatty()
        * encoding
        * mode
        * name
        * newlines

    Unlike standard file objects, all read methods share the same buffer
    and so can be freely mixed (e.g. read(), readline(), next(), ...).

    This class understands and will accept the following mode strings,
    with any additional characters being ignored:

        * r    - open the file for reading only.
        * r+   - open the file for reading and writing.
        * r-   - open the file for streamed reading; do not allow seek/tell.
        * w    - open the file for writing only; create the file if
                 it doesn't exist; truncate it to zero length.
        * w+   - open the file for reading and writing; create the file
                 if it doesn't exist; truncate it to zero length.
        * w-   - open the file for streamed writing; do not allow seek/tell.
        * a    - open the file for writing only; create the file if it
                 doesn't exist; place pointer at end of file.
        * a+   - open the file for reading and writing; create the file
                 if it doesn't exist; place pointer at end of file.

    These are mostly standard except for the "-" indicator, which has
    been added for efficiency purposes in cases where seeking can be
    expensive to simulate (e.g. compressed files).  Note that any file
    opened for both reading and writing must also support seeking.
    """

    def __init__(self,bufsize=1024*64):
        """FileLikeBase Constructor.

        The optional argument 'bufsize' specifies the number of bytes to
        read at a time when looking for a newline character.  Setting this to
        a larger number when lines are long should improve efficiency.
        """
        super(FileLikeBase, self).__init__()
        # File-like attributes
        self.closed = False
        self.softspace = 0
        # Our own attributes
        self._bufsize = bufsize  # buffer size for chunked reading
        self._rbuffer = None     # data that's been read but not returned
        self._wbuffer = None     # data that's been given but not written
        self._sbuffer = None     # data between real & apparent file pos
        self._soffset = 0        # internal offset of file pointer

    #
    #  The following five methods are the ones that subclasses are expected
    #  to implement.  Carefully check their docstrings.
    #

    def _read(self,sizehint=-1):
        """Read approximately <sizehint> bytes from the file-like object.

        This method is to be implemented by subclasses that wish to be
        readable.  It should read approximately <sizehint> bytes from the
        file and return them as a string.  If <sizehint> is missing or
        less than or equal to zero, try to read all the remaining contents.

        The method need not guarantee any particular number of bytes -
        it may return more bytes than requested, or fewer.  If needed the
        size hint may be completely ignored.  It may even return an empty
        string if no data is yet available.

        Because of this, the method must return None to signify that EOF
        has been reached.  The higher-level methods will never indicate EOF
        until None has been read from _read().  Once EOF is reached, it
        should be safe to call _read() again, immediately returning None.
        """
        raise NotReadableError("Object not readable")

    def _write(self,string,flushing=False):
        """Write the given string to the file-like object.

        This method must be implemented by subclasses wishing to be writable.
        It must attempt to write as much of the given data as possible to the
        file, but need not guarantee that it is all written.  It may return
        None to indicate that all data was written, or return as a string any
        data that could not be written.

        If the keyword argument 'flushing' is true, it indicates that the
        internal write buffers are being flushed, and *all* the given data
        is expected to be written to the file. If unwritten data is returned
        when 'flushing' is true, an IOError will be raised.
        """
        raise NotWritableError("Object not writable")

    def _seek(self,offset,whence):
        """Set the file's internal position pointer, approximately.

        This method should set the file's position to approximately 'offset'
        bytes relative to the position specified by 'whence'.  If it is
        not possible to position the pointer exactly at the given offset,
        it should be positioned at a convenient *smaller* offset and the
        file data between the real and apparent position should be returned.

        At minimum, this method must implement the ability to seek to
        the start of the file, i.e. offset=0 and whence=0.  If more
        complex seeks are difficult to implement then it may raise
        NotImplementedError to have them simulated (inefficiently) by
        the higher-level machinery of this class.
        """
        raise NotSeekableError("Object not seekable")

    def _tell(self):
        """Get the location of the file's internal position pointer.

        This method must be implemented by subclasses that wish to be
        seekable, and must return the position of the file's internal
        pointer.

        Due to buffering, the position seen by users of this class
        (the "apparent position") may be different to the position
        returned by this method (the "actual position").
        """
        raise NotSeekableError("Object not seekable")

    def _truncate(self,size):
        """Truncate the file's size to <size>.

        This method must be implemented by subclasses that wish to be
        truncatable.  It must truncate the file to exactly the given size
        or fail with an IOError.

        Note that <size> will never be None; if it was not specified by the
        user then it is calculated as the file's apparent position (which may
        be different to its actual position due to buffering).
        """
        raise NotTruncatableError("Object not truncatable")

    #
    #  The following methods provide the public API of the filelike object.
    #  Subclasses shouldn't need to mess with these (except perhaps for
    #  close() and flush())
    #

    def _check_mode(self,mode,mstr=None):
        """Check whether the file may be accessed in the given mode.

        'mode' must be one of "r" or "w", and this function returns False
        if the file-like object has a 'mode' attribute, and it does not
        permit access in that mode.  If there is no 'mode' attribute,
        it defaults to "r+".

        If seek support is not required, use "r-" or "w-" as the mode string.

        To check a mode string other than self.mode, pass it in as the
        second argument.
        """
        if mstr is None:
            try:
                mstr = self.mode
            except AttributeError:
                mstr = "r+"
        if "+" in mstr:
            return True
        if "-" in mstr and "-" not in mode:
            return False
        if "r" in mode:
            if "r" not in mstr:
                return False
        if "w" in mode:
            if "w" not in mstr and "a" not in mstr:
                return False
        return True

    def _assert_mode(self,mode,mstr=None):
        """Check whether the file may be accessed in the given mode.

        This method is equivalent to _check_assert(), but raises IOError
        instead of returning False.
        """
        if mstr is None:
            try:
                mstr = self.mode
            except AttributeError:
                mstr = "r+"
        if "+" in mstr:
            return True
        if "-" in mstr and "-" not in mode:
            raise NotSeekableError("File does not support seeking.")
        if "r" in mode:
            if "r" not in mstr:
                raise NotReadableError("File not opened for reading")
        if "w" in mode:
            if "w" not in mstr and "a" not in mstr:
                raise NotWritableError("File not opened for writing")
        return True

    def flush(self):
        """Flush internal write buffer, if necessary."""
        if self.closed:
            raise IOError("File has been closed")
        if self._check_mode("w-") and self._wbuffer is not None:
            buffered = b("")
            if self._sbuffer:
                buffered = buffered + self._sbuffer
                self._sbuffer = None
            buffered = buffered + self._wbuffer
            self._wbuffer = None
            leftover = self._write(buffered,flushing=True)
            if leftover and not isinstance(leftover, int):
                raise IOError("Could not flush write buffer.")

    def close(self):
        """Flush write buffers and close the file.

        The file may not be accessed further once it is closed.
        """
        #  Errors in subclass constructors can cause this to be called without
        #  having called FileLikeBase.__init__().  Since we need the attrs it
        #  initializes in cleanup, ensure we call it here.
        if not hasattr(self,"closed"):
            FileLikeBase.__init__(self)
        if not self.closed:
            self.flush()
            self.closed = True

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self,exc_type,exc_val,exc_tb):
        self.close()
        return False

    def next(self):
        """next() method complying with the iterator protocol.

        File-like objects are their own iterators, with each call to
        next() returning subsequent lines from the file.
        """
        ln = self.readline()
        if ln == b(""):
            raise StopIteration()
        return ln

    def __iter__(self):
        return self

    def truncate(self,size=None):
        """Truncate the file to the given size.

        If <size> is not specified or is None, the current file position is
        used.  Note that this method may fail at runtime if the underlying
        filelike object is not truncatable.
        """
        if "-" in getattr(self,"mode",""):
            raise NotTruncatableError("File is not seekable, can't truncate.")
        if self._wbuffer:
            self.flush()
        if size is None:
            size = self.tell()
        self._truncate(size)

    def seek(self,offset,whence=0):
        """Move the internal file pointer to the given location."""
        if whence > 2 or whence < 0:
            raise ValueError("Invalid value for 'whence': " + str(whence))
        if "-" in getattr(self,"mode",""):
            raise NotSeekableError("File is not seekable.")
        # Ensure that there's nothing left in the write buffer
        if self._wbuffer:
            self.flush()
        # Adjust for any data left in the read buffer
        if whence == 1 and self._rbuffer:
            offset = offset - len(self._rbuffer)
        self._rbuffer = None
        # Adjust for any discrepancy in actual vs apparent seek position
        if whence == 1:
            if self._sbuffer:
                offset = offset + len(self._sbuffer)
            if self._soffset:
                offset = offset + self._soffset
        self._sbuffer = None
        self._soffset = 0
        # Shortcut the special case of staying put.
        # As per posix, this has already cases the buffers to be flushed.
        if offset == 0 and whence == 1:
            return
        # Catch any failed attempts to read while simulating seek
        try:
            # Try to do a whence-wise seek if it is implemented.
            sbuf = None
            try:
                sbuf = self._seek(offset,whence)
            except NotImplementedError:
                # Try to simulate using an absolute seek.
                try:
                    if whence == 1:
                        offset = self._tell() + offset
                    elif whence == 2:
                        if hasattr(self,"size"):
                            offset = self.size + offset
                        else:
                            self._do_read_rest()
                            offset = self.tell() + offset
                    else:
                        # absolute seek already failed, don't try again
                        raise NotImplementedError
                    sbuf = self._seek(offset,0)
                except NotImplementedError:
                    # Simulate by reseting to start
                    self._seek(0,0)
                    self._soffset = offset
            finally:
                self._sbuffer = sbuf
        except NotReadableError:
            raise NotSeekableError("File not readable, can't simulate seek")

    def tell(self):
        """Determine current position of internal file pointer."""
        # Need to adjust for unread/unwritten data in buffers
        pos = self._tell()
        if self._rbuffer:
            pos = pos - len(self._rbuffer)
        if self._wbuffer:
            pos = pos + len(self._wbuffer)
        if self._sbuffer:
            pos = pos + len(self._sbuffer)
        if self._soffset:
            pos = pos + self._soffset
        return pos

    def read(self,size=-1):
        """Read at most 'size' bytes from the file.

        Bytes are returned as a string.  If 'size' is negative, zero or
        missing, the remainder of the file is read.  If EOF is encountered
        immediately, the empty string is returned.
        """
        if self.closed:
            raise IOError("File has been closed")
        self._assert_mode("r-")
        return self._do_read(size)

    def _do_read(self,size):
        """Private method to read from the file.

        This method behaves the same as self.read(), but skips some
        permission and sanity checks.  It is intended for use in simulating
        seek(), where we may want to read (and discard) information from
        a file not opened in read mode.

        Note that this may still fail if the file object actually can't
        be read from - it just won't check whether the mode string gives
        permission.
        """
        # If we were previously writing, ensure position is correct
        if self._wbuffer is not None:
            self.seek(0,1)
        # Discard any data that should have been seeked over
        if self._sbuffer:
            s = len(self._sbuffer)
            self._sbuffer = None
            self.read(s)
        elif self._soffset:
            s = self._soffset
            self._soffset = 0
            while s > self._bufsize:
                self._do_read(self._bufsize)
                s -= self._bufsize
            self._do_read(s)
        # Should the entire file be read?
        if size < 0:
            if self._rbuffer:
                data = [self._rbuffer]
            else:
                data = []
            self._rbuffer = b("")
            newData = self._read()
            while newData is not None:
                data.append(newData)
                newData = self._read()
            output = b("").join(data)
        # Otherwise, we need to return a specific amount of data
        else:
            if self._rbuffer:
                newData = self._rbuffer
                data = [newData]
            else:
                newData = b("")
                data = []
            sizeSoFar = len(newData)
            while sizeSoFar < size:
                newData = self._read(size-sizeSoFar)
                if not newData:
                    break
                data.append(newData)
                sizeSoFar += len(newData)
            data = b("").join(data)
            if sizeSoFar > size:
                # read too many bytes, store in the buffer
                self._rbuffer = data[size:]
                data = data[:size]
            else:
                self._rbuffer = b("")
            output = data
        return output

    def _do_read_rest(self):
        """Private method to read the file through to EOF."""
        data = self._do_read(self._bufsize)
        while data != b(""):
            data = self._do_read(self._bufsize)

    def readline(self,size=-1):
        """Read a line from the file, or at most <size> bytes."""
        bits = []
        indx = -1
        sizeSoFar = 0
        while indx == -1:
            nextBit = self.read(self._bufsize)
            bits.append(nextBit)
            sizeSoFar += len(nextBit)
            if not nextBit:
                break
            if size > 0 and sizeSoFar >= size:
                break
            indx = nextBit.find(b("\n"))
        # If not found, return whole string up to <size> length
        # Any leftovers are pushed onto front of buffer
        if indx == -1:
            data = b("").join(bits)
            if size > 0 and sizeSoFar > size:
                extra = data[size:]
                data = data[:size]
                self._rbuffer = extra + self._rbuffer
            return data
        # If found, push leftovers onto front of buffer
        # Add one to preserve the newline in the return value
        indx += 1
        extra = bits[-1][indx:]
        bits[-1] = bits[-1][:indx]
        self._rbuffer = extra + self._rbuffer
        return b("").join(bits)

    def readlines(self,sizehint=-1):
        """Return a list of all lines in the file."""
        return [ln for ln in self]

    def xreadlines(self):
        """Iterator over lines in the file - equivalent to iter(self)."""
        return iter(self)

    def write(self,string):
        """Write the given string to the file."""
        if self.closed:
            raise IOError("File has been closed")
        self._assert_mode("w-")
        # If we were previously reading, ensure position is correct
        if self._rbuffer is not None:
            self.seek(0, 1)
        # If we're actually behind the apparent position, we must also
        # write the data in the gap.
        if self._sbuffer:
            string = self._sbuffer + string
            self._sbuffer = None
        elif self._soffset:
            s = self._soffset
            self._soffset = 0
            try:
                string = self._do_read(s) + string
            except NotReadableError:
                raise NotSeekableError("File not readable, could not complete simulation of seek")
            self.seek(0, 0)
        if self._wbuffer:
            string = self._wbuffer + string
        leftover = self._write(string)
        if leftover is None or isinstance(leftover, int):
            self._wbuffer = b("")
            return len(string) - (leftover or 0)
        else:
            self._wbuffer = leftover
            return len(string) - len(leftover)

    def writelines(self,seq):
        """Write a sequence of lines to the file."""
        for ln in seq:
            self.write(ln)


class FileWrapper(FileLikeBase):
    """Base class for objects that wrap a file-like object.

    This class provides basic functionality for implementing file-like
    objects that wrap another file-like object to alter its functionality
    in some way.  It takes care of house-keeping duties such as flushing
    and closing the wrapped file.

    Access to the wrapped file is given by the attribute wrapped_file.
    By convention, the subclass's constructor should accept this as its
    first argument and pass it to its superclass's constructor in the
    same position.

    This class provides a basic implementation of _read() and _write()
    which just calls read() and write() on the wrapped object.  Subclasses
    will probably want to override these.
    """

    _append_requires_overwrite = False

    def __init__(self,wrapped_file,mode=None):
        """FileWrapper constructor.

        'wrapped_file' must be a file-like object, which is to be wrapped
        in another file-like object to provide additional functionality.

        If given, 'mode' must be the access mode string under which
        the wrapped file is to be accessed.  If not given or None, it
        is looked up on the wrapped file if possible.  Otherwise, it
        is not set on the object.
        """
        # This is used for working around flush/close inefficiencies
        self.__closing = False
        super(FileWrapper,self).__init__()
        self.wrapped_file = wrapped_file
        if mode is None:
            self.mode = getattr(wrapped_file,"mode","r+")
        else:
            self.mode = mode
        self._validate_mode()
        # Copy useful attributes of wrapped_file
        if hasattr(wrapped_file,"name"):
            self.name = wrapped_file.name
        # Respect append-mode setting
        if "a" in self.mode:
            if self._check_mode("r"):
                self.wrapped_file.seek(0)
            self.seek(0,2)

    def _validate_mode(self):
        """Check that various file-mode conditions are satisfied."""
        #  If append mode requires overwriting the underlying file,
        #  if must not be opened in append mode.
        if self._append_requires_overwrite:
            if self._check_mode("w"):
                if "a" in getattr(self.wrapped_file,"mode",""):
                    raise ValueError("Underlying file can't be in append mode")

    def __del__(self):
        #  Errors in subclass constructors could result in this being called
        #  without invoking FileWrapper.__init__.  Establish some simple
        #  invariants to prevent errors in this case.
        if not hasattr(self,"wrapped_file"):
            self.wrapped_file = None
        if not hasattr(self,"_FileWrapper__closing"):
            self.__closing = False
        #  Close the wrapper and the underlying file independently, so the
        #  latter is still closed on cleanup even if the former errors out.
        try:
            if FileWrapper is not None:
                super(FileWrapper,self).close()
        finally:
            if hasattr(getattr(self,"wrapped_file",None),"close"):
                self.wrapped_file.close()

    def close(self):
        """Close the object for reading/writing."""
        #  The superclass implementation of this will call flush(),
        #  which calls flush() on our wrapped object.  But we then call
        #  close() on it, which will call its flush() again!  To avoid
        #  this inefficiency, our flush() will not flush the wrapped
        #  file when we're closing.
        if not self.closed:
            self.__closing = True
            super(FileWrapper,self).close()
            if hasattr(self.wrapped_file,"close"):
                self.wrapped_file.close()

    def flush(self):
        """Flush the write buffers of the file."""
        super(FileWrapper,self).flush()
        if not self.__closing and hasattr(self.wrapped_file,"flush"):
            self.wrapped_file.flush()

    def _read(self,sizehint=-1):
        data = self.wrapped_file.read(sizehint)
        if data == b(""):
            return None
        return data

    def _write(self,string,flushing=False):
        self.wrapped_file.write(string)

    def _seek(self,offset,whence):
        self.wrapped_file.seek(offset,whence)

    def _tell(self):
        return self.wrapped_file.tell()

    def _truncate(self,size):
        return self.wrapped_file.truncate(size)


class StringIO(FileWrapper):
    """StringIO wrapper that more closely matches standard file behavior.

    This is a simple compatibility wrapper around the native StringIO class
    which fixes some corner-cases of its behavior.  Specifically:

        * adding __enter__ and __exit__ methods
        * having truncate(size) zero-fill when growing the file

    """

    def __init__(self,data=None,mode=None):
        wrapped_file = _StringIO()
        if data is not None:
            wrapped_file.write(data)
            wrapped_file.seek(0)
        super(StringIO,self).__init__(wrapped_file,mode)

    def getvalue(self):
        return self.wrapped_file.getvalue()

    def _truncate(self,size):
        pos = self.wrapped_file.tell()
        self.wrapped_file.truncate(size)
        curlen = len(self.wrapped_file.getvalue())
        if size > curlen:
            self.wrapped_file.seek(curlen)
            try:
                self.wrapped_file.write(b("\x00")*(size-curlen))
            finally:
                self.wrapped_file.seek(pos)


class SpooledTemporaryFile(FileWrapper):
    """SpooledTemporaryFile wrapper with some compatibility fixes.

    This is a simple compatibility wrapper around the native class of the
    same name, fixing some corner-cases of its behavior.  Specifically:

        * have truncate() accept a size argument
        * roll to disk is seeking past the max in-memory size
        * use improved StringIO class from this module

    """

    def __init__(self,max_size=0,mode="w+b",bufsize=-1,*args,**kwds):
        try:
            stf_args = (max_size,mode,bufsize) + args
            wrapped_file = _tempfile.SpooledTemporaryFile(*stf_args,**kwds)
            wrapped_file._file = StringIO()
            #wrapped_file._file = six.BytesIO()
            self.__is_spooled = True
        except AttributeError:
            ntf_args = (mode,bufsize) + args
            wrapped_file = _tempfile.NamedTemporaryFile(*ntf_args,**kwds)
            self.__is_spooled = False
        super(SpooledTemporaryFile,self).__init__(wrapped_file)

    def _seek(self,offset,whence):
        if self.__is_spooled:
            max_size = self.wrapped_file._max_size
            if whence == fs.SEEK_SET:
                if offset > max_size:
                    self.wrapped_file.rollover()
            elif whence == fs.SEEK_CUR:
                if offset + self.wrapped_file.tell() > max_size:
                    self.wrapped_file.rollover()
            else:
                if offset > 0:
                    self.wrapped_file.rollover()
        self.wrapped_file.seek(offset,whence)

    def _truncate(self,size):
        if self.__is_spooled:
            self.wrapped_file._file.truncate(size)
        else:
            self.wrapped_file.truncate(size)

    def fileno(self):
        return self.wrapped_file.fileno()


class LimitBytesFile(FileWrapper):
    """Filelike wrapper to limit bytes read from a stream."""

    def __init__(self,size,fileobj,*args,**kwds):
        self.size = size
        self.__remaining = size
        super(LimitBytesFile,self).__init__(fileobj,*args,**kwds)

    def _read(self,sizehint=-1):
        if self.__remaining <= 0:
            return None
        if sizehint is None or sizehint < 0 or sizehint > self.__remaining:
            sizehint = self.__remaining
        data = super(LimitBytesFile,self)._read(sizehint)
        if data is not None:
            self.__remaining -= len(data)
        return data


