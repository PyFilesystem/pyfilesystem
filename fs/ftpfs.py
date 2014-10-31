"""
fs.ftpfs
========

FTPFS is a filesystem for accessing an FTP server (uses ftplib in standard library)

"""

__all__ = ['FTPFS']

import sys

import fs
from fs.base import *
from fs.errors import *
from fs.path import pathsplit, abspath, dirname, recursepath, normpath, pathjoin, isbase
from fs import iotools

from ftplib import FTP, error_perm, error_temp, error_proto, error_reply

try:
    from ftplib import _GLOBAL_DEFAULT_TIMEOUT
except ImportError:
    _GLOBAL_DEFAULT_TIMEOUT = object()

import threading
import datetime
import calendar

from socket import error as socket_error
from fs.local_functools import wraps

import six
from six import PY3, b

if PY3:
    from six import BytesIO as StringIO
else:
    try:
        from cStringIO import StringIO
    except ImportError:
        from StringIO import StringIO

import time


# -----------------------------------------------
# Taken from http://www.clapper.org/software/python/grizzled/
# -----------------------------------------------

class Enum(object):
    def __init__(self, *names):
        self._names_map = dict((name, i) for i, name in enumerate(names))

    def __getattr__(self, name):
        return self._names_map[name]

MONTHS = ('jan', 'feb', 'mar', 'apr', 'may', 'jun',
          'jul', 'aug', 'sep', 'oct', 'nov', 'dec')

MTIME_TYPE = Enum('UNKNOWN', 'LOCAL', 'REMOTE_MINUTE', 'REMOTE_DAY')
"""
``MTIME_TYPE`` identifies how a modification time ought to be interpreted
(assuming the caller cares).

    - ``LOCAL``: Time is local to the client, granular to (at least) the minute
    - ``REMOTE_MINUTE``: Time is local to the server and granular to the minute
    - ``REMOTE_DAY``: Time is local to the server and granular to the day.
    - ``UNKNOWN``: Time's locale is unknown.
"""

ID_TYPE = Enum('UNKNOWN', 'FULL')
"""
``ID_TYPE`` identifies how a file's identifier should be interpreted.

    - ``FULL``: The ID is known to be complete.
    - ``UNKNOWN``: The ID is not set or its type is unknown.
"""

# ---------------------------------------------------------------------------
# Globals
# ---------------------------------------------------------------------------

now = time.time()
current_year = time.localtime().tm_year

# ---------------------------------------------------------------------------
# Classes
# ---------------------------------------------------------------------------

class FTPListData(object):
    """
    The `FTPListDataParser` class's ``parse_line()`` method returns an
    instance of this class, capturing the parsed data.

    :IVariables:
        name : str
            The name of the file, if parsable
        try_cwd : bool
            ``True`` if the entry might be a directory (i.e., the caller
            might want to try an FTP ``CWD`` command), ``False`` if it
            cannot possibly be a directory.
        try_retr : bool
            ``True`` if the entry might be a retrievable file (i.e., the caller
            might want to try an FTP ``RETR`` command), ``False`` if it
            cannot possibly be a file.
        size : long
            The file's size, in bytes
        mtime : long
            The file's modification time, as a value that can be passed to
            ``time.localtime()``.
        mtime_type : `MTIME_TYPE`
            How to interpret the modification time. See `MTIME_TYPE`.
        id : str
            A unique identifier for the file. The unique identifier is unique
            on the *server*. On a Unix system, this identifier might be the
            device number and the file's inode; on other system's, it might
            be something else. It's also possible for this field to be ``None``.
        id_type : `ID_TYPE`
            How to interpret the identifier. See `ID_TYPE`.
   """

    def __init__(self, raw_line):
        self.raw_line = raw_line
        self.name = None
        self.try_cwd = False
        self.try_retr = False
        self.size = 0
        self.mtime_type = MTIME_TYPE.UNKNOWN
        self.mtime = 0
        self.id_type = ID_TYPE.UNKNOWN
        self.id = None

class FTPListDataParser(object):
    """
    An ``FTPListDataParser`` object can be used to parse one or more lines
    that were retrieved by an FTP ``LIST`` command that was sent to a remote
    server.
    """
    def __init__(self):
        pass

    def parse_line(self, ftp_list_line):
        """
        Parse a line from an FTP ``LIST`` command.

        :Parameters:
            ftp_list_line : str
                The line of output

        :rtype: `FTPListData`
        :return: An `FTPListData` object describing the parsed line, or
                 ``None`` if the line could not be parsed. Note that it's
                 possible for this method to return a partially-filled
                 `FTPListData` object (e.g., one without a name).
        """
        buf = ftp_list_line

        if len(buf) < 2: # an empty name in EPLF, with no info, could be 2 chars
            return None

        c = buf[0]
        if c == '+':
            return self._parse_EPLF(buf)

        elif c in 'bcdlps-':
            return self._parse_unix_style(buf)

        i = buf.find(';')
        if i > 0:
            return self._parse_multinet(buf, i)

        if c in '0123456789':
            return self._parse_msdos(buf)

        return None

    # UNIX ls does not show the year for dates in the last six months.
    # So we have to guess the year.
    #
    # Apparently NetWare uses ``twelve months'' instead of ``six months''; ugh.
    # Some versions of ls also fail to show the year for future dates.

    def _guess_time(self, month, mday, hour=0, minute=0):
        year = None
        t = None

        for year in range(current_year - 1, current_year + 100):
            t = self._get_mtime(year, month, mday, hour, minute)
            if (now - t) < (350 * 86400):
                return t

        return 0

    def _get_mtime(self, year, month, mday, hour=0, minute=0, second=0):
        return time.mktime((year, month, mday, hour, minute, second, 0, 0, -1))

    def _get_month(self, buf):
        if len(buf) == 3:
            for i in range(0, 12):
                if buf.lower().startswith(MONTHS[i]):
                    return i+1
        return -1

    def _parse_EPLF(self, buf):
        result = FTPListData(buf)

        # see http://cr.yp.to/ftp/list/eplf.html
        #"+i8388621.29609,m824255902,/,\tdev"
        #"+i8388621.44468,m839956783,r,s10376,\tRFCEPLF"
        i = 1
        for j in range(1, len(buf)):
            if buf[j] == '\t':
                result.name = buf[j+1:]
                break

            if buf[j] == ',':
                c = buf[i]
                if c == '/':
                    result.try_cwd = True
                elif c == 'r':
                    result.try_retr = True
                elif c == 's':
                    result.size = long(buf[i+1:j])
                elif c == 'm':
                    result.mtime_type = MTIME_TYPE.LOCAL
                    result.mtime = long(buf[i+1:j])
                elif c == 'i':
                    result.id_type = ID_TYPE.FULL
                    result.id = buf[i+1:j-i-1]

                i = j + 1

        return result

    def _parse_unix_style(self, buf):
        # UNIX-style listing, without inum and without blocks:
        # "-rw-r--r--   1 root     other        531 Jan 29 03:26 README"
        # "dr-xr-xr-x   2 root     other        512 Apr  8  1994 etc"
        # "dr-xr-xr-x   2 root     512 Apr  8  1994 etc"
        # "lrwxrwxrwx   1 root     other          7 Jan 25 00:17 bin -> usr/bin"
        #
        # Also produced by Microsoft's FTP servers for Windows:
        # "----------   1 owner    group         1803128 Jul 10 10:18 ls-lR.Z"
        # "d---------   1 owner    group               0 May  9 19:45 Softlib"
        #
        # Also WFTPD for MSDOS:
        # "-rwxrwxrwx   1 noone    nogroup      322 Aug 19  1996 message.ftp"
        #
        # Also NetWare:
        # "d [R----F--] supervisor            512       Jan 16 18:53    login"
        # "- [R----F--] rhesus             214059       Oct 20 15:27    cx.exe"
        #
        # Also NetPresenz for the Mac:
        # "-------r--         326  1391972  1392298 Nov 22  1995 MegaPhone.sit"
        # "drwxrwxr-x               folder        2 May 10  1996 network"

        result = FTPListData(buf)

        buflen = len(buf)
        c = buf[0]
        if c == 'd':
            result.try_cwd = True
        if c == '-':
            result.try_retr = True
        if c == 'l':
            result.try_retr = True
            result.try_cwd = True

        state = 1
        i = 0
        tokens = buf.split()
        for j in range(1, buflen):
            if (buf[j] == ' ') and (buf[j - 1] != ' '):
                if state == 1:  # skipping perm
                    state = 2

                elif state == 2: # skipping nlink
                    state = 3
                    if ((j - i) == 6) and (buf[i] == 'f'): # NetPresenz
                        state = 4

                elif state == 3: # skipping UID/GID
                    state = 4

                elif state == 4: # getting tentative size
                    try:
                        size = long(buf[i:j])
                    except ValueError:
                        pass
                    state = 5

                elif state == 5: # searching for month, else getting tentative size
                    month = self._get_month(buf[i:j])
                    if month >= 0:
                        state = 6
                    else:
                        size = long(buf[i:j])

                elif state == 6: # have size and month
                    mday = long(buf[i:j])
                    state = 7

                elif state == 7: # have size, month, mday
                    if (j - i == 4) and (buf[i+1] == ':'):
                        hour = long(buf[i])
                        minute = long(buf[i+2:i+4])
                        result.mtime_type = MTIME_TYPE.REMOTE_MINUTE
                        result.mtime = self._guess_time(month, mday, hour, minute)
                    elif (j - i == 5) and (buf[i+2] == ':'):
                        hour = long(buf[i:i+2])
                        minute = long(buf[i+3:i+5])
                        result.mtime_type = MTIME_TYPE.REMOTE_MINUTE
                        result.mtime = self._guess_time(month, mday, hour, minute)
                    elif j - i >= 4:
                        year = long(buf[i:j])
                        result.mtime_type = MTIME_TYPE.REMOTE_DAY
                        result.mtime = self._get_mtime(year, month, mday)
                    else:
                        break

                    result.name = buf[j+1:]
                    state = 8
                elif state == 8: # twiddling thumbs
                    pass

                i = j + 1
                while (i < buflen) and (buf[i] == ' '):
                    i += 1

        #if state != 8:
            #return None

        result.size = size

        if c == 'l':
            i = 0
            while (i + 3) < len(result.name):
                if result.name[i:i+4] == ' -> ':
                    result.target = result.name[i+4:]
                    result.name = result.name[:i]
                    break
                i += 1

        # eliminate extra NetWare spaces
        if (buf[1] == ' ') or (buf[1] == '['):
            namelen = len(result.name)
            if namelen > 3:
                result.name = result.name.strip()

        return result

    def _parse_multinet(self, buf, i):

        # MultiNet (some spaces removed from examples)
        # "00README.TXT;1      2 30-DEC-1996 17:44 [SYSTEM] (RWED,RWED,RE,RE)"
        # "CORE.DIR;1          1  8-SEP-1996 16:09 [SYSTEM] (RWE,RWE,RE,RE)"
        # and non-MultiNet VMS:
        #"CII-MANUAL.TEX;1  213/216  29-JAN-1996 03:33:12  [ANONYMOU,ANONYMOUS]   (RWED,RWED,,)"

        result = FTPListData(buf)
        result.name = buf[:i]
        buflen = len(buf)

        if i > 4:
            if buf[i-4:i] == '.DIR':
                result.name = result.name[0:-4]
                result.try_cwd = True

        if not result.try_cwd:
            result.try_retr = True

        try:
            i = buf.index(' ', i)
            i = _skip(buf, i, ' ')
            i = buf.index(' ', i)
            i = _skip(buf, i, ' ')

            j = i

            j = buf.index('-', j)
            mday = long(buf[i:j])

            j = _skip(buf, j, '-')
            i = j
            j = buf.index('-', j)
            month = self._get_month(buf[i:j])
            if month < 0:
                raise IndexError

            j = _skip(buf, j, '-')
            i = j
            j = buf.index(' ', j)
            year = long(buf[i:j])

            j = _skip(buf, j, ' ')
            i = j

            j = buf.index(':', j)
            hour = long(buf[i:j])
            j = _skip(buf, j, ':')
            i = j

            while (buf[j] != ':') and (buf[j] != ' '):
                j += 1
                if j == buflen:
                    raise IndexError # abort, abort!

            minute = long(buf[i:j])

            result.mtime_type = MTIME_TYPE.REMOTE_MINUTE
            result.mtime = self._get_mtime(year, month, mday, hour, minute)

        except IndexError:
            pass

        return result

    def _parse_msdos(self, buf):
        # MSDOS format
        # 04-27-00  09:09PM       <DIR>          licensed
        # 07-18-00  10:16AM       <DIR>          pub
        # 04-14-00  03:47PM                  589 readme.htm

        buflen = len(buf)
        i = 0
        j = 0

        try:
            result = FTPListData(buf)

            j = buf.index('-', j)
            month = long(buf[i:j])

            j = _skip(buf, j, '-')
            i = j
            j = buf.index('-', j)
            mday = long(buf[i:j])

            j = _skip(buf, j, '-')
            i = j
            j = buf.index(' ', j)
            year = long(buf[i:j])
            if year < 50:
                year += 2000
            if year < 1000:
                year += 1900

            j = _skip(buf, j, ' ')
            i = j
            j = buf.index(':', j)
            hour = long(buf[i:j])
            j = _skip(buf, j, ':')
            i = j
            while not (buf[j] in 'AP'):
                j += 1
                if j == buflen:
                    raise IndexError
            minute = long(buf[i:j])

            if buf[j] == 'A':
                j += 1
                if j == buflen:
                    raise IndexError

            if buf[j] == 'P':
                hour = (hour + 12) % 24
                j += 1
                if j == buflen:
                    raise IndexError

            if buf[j] == 'M':
                j += 1
                if j == buflen:
                    raise IndexError

            j = _skip(buf, j, ' ')
            if buf[j] == '<':
                result.try_cwd = True
                j = buf.index(' ', j)
            else:
                i = j
                j = buf.index(' ', j)

                result.size = long(buf[i:j])
                result.try_retr = True

            j = _skip(buf, j, ' ')

            result.name = buf[j:]
            result.mtime_type = MTIME_TYPE.REMOTE_MINUTE
            result.mtime = self._get_mtime(year, month, mday, hour, minute)
        except IndexError:
            pass

        return result

class FTPMlstDataParser(object):
    """
    An ``FTPMlstDataParser`` object can be used to parse one or more lines
    that were retrieved by an FTP ``MLST`` or ``MLSD`` command that was sent
    to a remote server.
    """
    def __init__(self):
        pass

    def parse_line(self, ftp_list_line):
        """
        Parse a line from an FTP ``MLST`` or ``MLSD`` command.

        :Parameters:
            ftp_list_line : str
                The line of output

        :rtype: `FTPListData`
        :return: An `FTPListData` object describing the parsed line, or
                 ``None`` if the line could not be parsed. Note that it's
                 possible for this method to return a partially-filled
                 `FTPListData` object (e.g., one without a mtime).
        """
        result = FTPListData(ftp_list_line)
        # pull out the name
        parts = ftp_list_line.partition(' ')
        result.name = parts[2]

        # parse the facts
        if parts[0][-1] == ';':
            for fact in parts[0][:-1].split(';'):
                parts = fact.partition('=')
                factname = parts[0].lower()
                factvalue = parts[2]
                if factname == 'unique':
                    if factvalue == "0g0" or factvalue == "0g1":
                        # Matrix FTP server sometimes returns bogus "unique" facts
                        result.id_type = ID_TYPE.UNKNOWN
                    else:
                        result.id_type = ID_TYPE.FULL
                    result.id = factvalue
                elif factname == 'modify':
                    result.mtime_type = MTIME_TYPE.LOCAL
                    result.mtime = calendar.timegm((int(factvalue[0:4]),
                                                     int(factvalue[4:6]),
                                                     int(factvalue[6:8]),
                                                     int(factvalue[8:10]),
                                                     int(factvalue[10:12]),
                                                     int(factvalue[12:14]),
                                                     0, 0, 0))
                elif factname == 'size':
                    result.size = long(factvalue)
                elif factname == 'sizd':
                    # some FTP servers report directory size with sizd
                    result.size = long(factvalue)
                elif factname == 'type':
                    if factvalue.lower() == 'file':
                        result.try_retr = True
                    elif factvalue.lower() in ['dir', 'cdir', 'pdir']:
                        result.try_cwd = True
                    else:
                        # dunno if it's file or directory
                        result.try_retr = True
                        result.try_cwd = True
        return result

# ---------------------------------------------------------------------------
# Public Functions
# ---------------------------------------------------------------------------

def parse_ftp_list_line(ftp_list_line, is_mlst=False):
    """
    Convenience function that instantiates an `FTPListDataParser` object
    and passes ``ftp_list_line`` to the object's ``parse_line()`` method,
    returning the result.

    :Parameters:
        ftp_list_line : str
            The line of output

    :rtype: `FTPListData`
    :return: An `FTPListData` object describing the parsed line, or
             ``None`` if the line could not be parsed. Note that it's
             possible for this method to return a partially-filled
             `FTPListData` object (e.g., one without a name).
    """
    if is_mlst:
        return FTPMlstDataParser().parse_line(ftp_list_line)
    else:
        return FTPListDataParser().parse_line(ftp_list_line)

# ---------------------------------------------------------------------------
# Private Functions
# ---------------------------------------------------------------------------

def _skip(s, i, c):
    while s[i] == c:
        i += 1
        if i == len(s):
            raise IndexError
    return i


def fileftperrors(f):
    @wraps(f)
    def deco(self, *args, **kwargs):
        self._lock.acquire()
        try:
            try:
                ret = f(self, *args, **kwargs)
            except Exception, e:
                self.ftpfs._translate_exception(args[0] if args else '', e)
        finally:
            self._lock.release()
        return ret
    return deco



class _FTPFile(object):

    """ A file-like that provides access to a file being streamed over ftp."""

    blocksize = 1024 * 64

    def __init__(self, ftpfs, ftp, path, mode):
        if not hasattr(self, '_lock'):
            self._lock = threading.RLock()
        self.ftpfs = ftpfs
        self.ftp = ftp
        self.path = normpath(path)
        self.mode = mode
        self.read_pos = 0
        self.write_pos = 0
        self.closed = False
        self.file_size = None
        if 'r' in mode or 'a' in mode:
            self.file_size = ftpfs.getsize(path)
        self.conn = None

        self._start_file(mode, _encode(self.path))

    @fileftperrors
    def _start_file(self, mode, path):
        self.read_pos = 0
        self.write_pos = 0
        if 'r' in mode:
            self.ftp.voidcmd('TYPE I')
            self.conn = self.ftp.transfercmd('RETR ' + path, None)

        else:#if 'w' in mode or 'a' in mode:
            self.ftp.voidcmd('TYPE I')
            if 'a' in mode:
                self.write_pos = self.file_size
                self.conn = self.ftp.transfercmd('APPE ' + path)
            else:
                self.conn = self.ftp.transfercmd('STOR ' + path)

    @fileftperrors
    def read(self, size=None):
        if self.conn is None:
            return b('')

        chunks = []
        if size is None or size < 0:
            while 1:
                data = self.conn.recv(self.blocksize)
                if not data:
                    self.conn.close()
                    self.conn = None
                    self.ftp.voidresp()
                    break
                chunks.append(data)
                self.read_pos += len(data)
            return b('').join(chunks)

        remaining_bytes = size
        while remaining_bytes:
            read_size = min(remaining_bytes, self.blocksize)
            data = self.conn.recv(read_size)
            if not data:
                self.conn.close()
                self.conn = None
                self.ftp.voidresp()
                break
            chunks.append(data)
            self.read_pos += len(data)
            remaining_bytes -= len(data)

        return b('').join(chunks)

    @fileftperrors
    def write(self, data):

        data_pos = 0
        remaining_data = len(data)

        while remaining_data:
            chunk_size = min(remaining_data, self.blocksize)
            self.conn.sendall(data[data_pos:data_pos+chunk_size])
            data_pos += chunk_size
            remaining_data -= chunk_size
            self.write_pos += chunk_size


    def __enter__(self):
        return self

    def __exit__(self,exc_type,exc_value,traceback):
        self.close()

    @fileftperrors
    def flush(self):
        self.ftpfs._on_file_written(self.path)

    @fileftperrors
    def seek(self, pos, where=fs.SEEK_SET):
        # Ftp doesn't support a real seek, so we close the transfer and resume
        # it at the new position with the REST command
        # I'm not sure how reliable this method is!
        if self.file_size is None:
            raise ValueError("Seek only works with files open for read")

        self._lock.acquire()
        try:

            current = self.tell()
            new_pos = None
            if where == fs.SEEK_SET:
                new_pos = pos
            elif where == fs.SEEK_CUR:
                new_pos = current + pos
            elif where == fs.SEEK_END:
                new_pos = self.file_size + pos
            if new_pos < 0:
                raise ValueError("Can't seek before start of file")

            if self.conn is not None:
                self.conn.close()

        finally:
            self._lock.release()

        self.close()
        self._lock.acquire()
        try:
            self.ftp = self.ftpfs._open_ftp()
            self.ftp.sendcmd('TYPE I')
            self.ftp.sendcmd('REST %i' % (new_pos))
            self.__init__(self.ftpfs, self.ftp, self.path, self.mode)
            self.read_pos = new_pos
        finally:
            self._lock.release()

        #raise UnsupportedError('ftp seek')

    @fileftperrors
    def tell(self):
        if 'r' in self.mode:
            return self.read_pos
        else:
            return self.write_pos

    @fileftperrors
    def truncate(self, size=None):
        self.ftpfs._on_file_written(self.path)
        # Inefficient, but I don't know how else to implement this
        if size is None:
            size = self.tell()

        if self.conn is not None:
            self.conn.close()
        self.close()

        read_f = None
        try:
            read_f = self.ftpfs.open(self.path, 'rb')
            data = read_f.read(size)
        finally:
            if read_f is not None:
                read_f.close()

        self.ftp = self.ftpfs._open_ftp()
        self.mode = 'w'
        self.__init__(self.ftpfs, self.ftp, _encode(self.path), self.mode)
        #self._start_file(self.mode, self.path)
        self.write(data)
        if len(data) < size:
            self.write('\0' * (size - len(data)))


    @fileftperrors
    def close(self):
        if 'w' in self.mode or 'a' in self.mode or '+' in self.mode:
            self.ftpfs._on_file_written(self.path)
        if self.conn is not None:
            try:
                self.conn.close()
                self.conn = None
                self.ftp.voidresp()
            except error_temp, error_perm:
                pass
        if self.ftp is not None:
            try:
                self.ftp.close()
            except error_temp, error_perm:
                pass
        self.closed = True

    def next(self):
        return self.readline()

    def readline(self, size=None):
        return next(iotools.line_iterator(self, size))

    def __iter__(self):
        return iotools.line_iterator(self)


def ftperrors(f):
    @wraps(f)
    def deco(self, *args, **kwargs):
        self._lock.acquire()
        try:
            self._enter_dircache()
            try:
                try:
                    ret = f(self, *args, **kwargs)
                except Exception, e:
                    self._translate_exception(args[0] if args else '', e)
            finally:
                self._leave_dircache()
        finally:
            self._lock.release()
        return ret
    return deco


def _encode(s):
    if isinstance(s, unicode):
        return s.encode('utf-8')
    return s

class _DirCache(dict):
    def __init__(self):
        super(_DirCache, self).__init__()
        self.count = 0

    def addref(self):
        self.count += 1
        return self.count

    def decref(self):
        self.count -= 1
        return self.count

class FTPFS(FS):

    _meta = { 'thread_safe' : True,
              'network' : True,
              'virtual': False,
              'read_only' : False,
              'unicode_paths' : True,
              'case_insensitive_paths' : False,
              'atomic.move' : True,
              'atomic.copy' : True,
              'atomic.makedir' : True,
              'atomic.rename' : True,
              'atomic.setcontents' : False,
              'file.read_and_write' : False,
              }

    def __init__(self, host='', user='', passwd='', acct='', timeout=_GLOBAL_DEFAULT_TIMEOUT, port=21, dircache=True, follow_symlinks=False):
        """Connect to a FTP server.

        :param host: Host to connect to
        :param user: Username, or a blank string for anonymous
        :param passwd: Password, if required
        :param acct: Accounting information (few servers require this)
        :param timeout: Timeout in seconds
        :param port: Port to connection (default is 21)
        :param dircache: If True then directory information will be cached,
            speeding up operations such as `getinfo`, `isdir`, `isfile`, but
            changes to the ftp file structure will not be visible until
            :meth:`~fs.ftpfs.FTPFS.clear_dircache` is called

        """

        super(FTPFS, self).__init__()

        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.acct = acct
        self.timeout = timeout
        self.default_timeout = timeout is _GLOBAL_DEFAULT_TIMEOUT
        self.use_dircache = dircache
        self.follow_symlinks = follow_symlinks

        self.use_mlst = False
        self._lock = threading.RLock()
        self._init_dircache()

        self._cache_hint = False
        try:
            self.ftp
        except FSError:
            self.closed = True
            raise

    def _init_dircache(self):
        self.dircache = _DirCache()

    @synchronize
    def cache_hint(self, enabled):
        self._cache_hint = bool(enabled)

    def _enter_dircache(self):
        self.dircache.addref()

    def _leave_dircache(self):
        self.dircache.decref()
        if self.use_dircache:
            if not self.dircache.count and not self._cache_hint:
                self.clear_dircache()
        else:
            self.clear_dircache()
        assert self.dircache.count >= 0, "dircache count should never be negative"

    @synchronize
    def _on_file_written(self, path):
        self.refresh_dircache(dirname(path))

    @synchronize
    def _readdir(self, path):
        path = abspath(normpath(path))
        if self.dircache.count:
            cached_dirlist = self.dircache.get(path)
            if cached_dirlist is not None:
                return cached_dirlist
        dirlist = {}

        def _get_FEAT(ftp):
            features = dict()
            try:
                response = ftp.sendcmd("FEAT")
                if response[:3] == "211":
                    for line in response.splitlines()[1:]:
                        if line[3] == "211":
                            break
                        if line[0] != ' ':
                            break
                        parts = line[1:].partition(' ')
                        features[parts[0].upper()] = parts[2]
            except error_perm:
                # some FTP servers may not support FEAT
                pass
            return features

        def on_line(line):
            if not isinstance(line, unicode):
                line = line.decode('utf-8')
            info = parse_ftp_list_line(line, self.use_mlst)
            if info:
                info = info.__dict__
                if info['name'] not in ('.', '..'):
                    dirlist[info['name']] = info

        try:
            encoded_path = _encode(path)
            ftp_features = _get_FEAT(self.ftp)
            if 'MLST' in ftp_features:
                self.use_mlst = True
                try:
                    # only request the facts we need
                    self.ftp.sendcmd("OPTS MLST type;unique;size;modify;")
                except error_perm:
                    # some FTP servers don't support OPTS MLST
                    pass
                # need to send MLST first to discover if it's file or dir
                response = self.ftp.sendcmd("MLST " + encoded_path)
                lines = response.splitlines()
                if lines[0][:3] == "250":
                    list_line = lines[1]
                    # MLST line is preceded by space
                    if list_line[0] == ' ':
                        on_line(list_line[1:])
                    else: # Matrix FTP server has bug
                        on_line(list_line)
                # if it's a dir, then we can send a MLSD
                if dirlist[dirlist.keys()[0]]['try_cwd']:
                    dirlist = {}
                    self.ftp.retrlines("MLSD " + encoded_path, on_line)
            else:
                self.ftp.dir(encoded_path, on_line)
        except error_reply:
            pass
        self.dircache[path] = dirlist

        def is_symlink(info):
            return info['try_retr'] and info['try_cwd'] and info.has_key('target')

        def resolve_symlink(linkpath):
            linkinfo = self.getinfo(linkpath)
            if not linkinfo.has_key('resolved'):
                linkinfo['resolved'] = linkpath
            if is_symlink(linkinfo):
                target = linkinfo['target']
                base, fname = pathsplit(linkpath)
                return resolve_symlink(pathjoin(base, target))
            else:
                return linkinfo

        if self.follow_symlinks:
            for name in dirlist:
                if is_symlink(dirlist[name]):
                    target = dirlist[name]['target']
                    linkinfo = resolve_symlink(pathjoin(path, target))
                    for key in linkinfo:
                        if key != 'name':
                            dirlist[name][key] = linkinfo[key]
                    del dirlist[name]['target']

        return dirlist

    @synchronize
    def clear_dircache(self, *paths):
        """
        Clear cached directory information.

        :param path: Path of directory to clear cache for, or all directories if
        None (the default)

        """

        if not paths:
            self.dircache.clear()
        else:
            dircache = self.dircache
            paths = [normpath(abspath(path)) for path in paths]
            for cached_path in dircache.keys():
                for path in paths:
                    if isbase(cached_path, path):
                        dircache.pop(cached_path, None)
                        break

    @synchronize
    def refresh_dircache(self, *paths):
        for path in paths:
            path = abspath(normpath(path))
            self.dircache.pop(path, None)

    @synchronize
    def _check_path(self, path):
        path = normpath(path)
        base, fname = pathsplit(abspath(path))
        dirlist = self._readdir(base)
        if fname and fname not in dirlist:
            raise ResourceNotFoundError(path)
        return dirlist, fname

    def _get_dirlist(self, path):
        path = normpath(path)
        base, fname = pathsplit(abspath(path))
        dirlist = self._readdir(base)
        return dirlist, fname


    @ftperrors
    def get_ftp(self):
        if self.closed:
            return None
        if not getattr(self, '_ftp', None):
            self._ftp = self._open_ftp()
        return self._ftp

    ftp = property(get_ftp)

    @ftperrors
    def _open_ftp(self):
        try:
            ftp = FTP()
            if self.default_timeout or sys.version_info < (2,6,):
                ftp.connect(self.host, self.port)
            else:
                ftp.connect(self.host, self.port, self.timeout)
            ftp.login(self.user, self.passwd, self.acct)
        except socket_error, e:
            raise RemoteConnectionError(str(e), details=e)
        return ftp

    def __getstate__(self):
        state = super(FTPFS, self).__getstate__()
        del state['_lock']
        state.pop('_ftp', None)
        return state

    def __setstate__(self,state):
        super(FTPFS, self).__setstate__(state)
        self._init_dircache()
        self._lock = threading.RLock()
        #self._ftp = None
        #self.ftp

    def __str__(self):
        return '<FTPFS %s>' % self.host

    def __unicode__(self):
        return u'<FTPFS %s>' % self.host

    @convert_os_errors
    def _translate_exception(self, path, exception):

        """ Translates exceptions that my be thrown by the ftp code in to
        FS exceptions

        TODO: Flesh this out with more specific exceptions

        """

        if isinstance(exception, socket_error):
            self._ftp = None
            raise RemoteConnectionError(str(exception), details=exception)

        elif isinstance(exception, error_temp):
            code, message = str(exception).split(' ', 1)
            self._ftp = None
            raise RemoteConnectionError(str(exception), path=path, msg="FTP error: %s" % str(exception), details=exception)

        elif isinstance(exception, error_perm):
            code, message = str(exception).split(' ', 1)
            code = int(code)
            if code == 550:
                pass
            if code == 552:
                raise StorageSpaceError
            raise PermissionDeniedError(str(exception), path=path, msg="FTP error: %s" % str(exception), details=exception)

        raise exception

    @ftperrors
    def close(self):
        if not self.closed:
            try:
                self.ftp.close()
            except FSError:
                pass
            self.closed = True

    def getpathurl(self, path, allow_none=False):
        path = normpath(path)
        credentials = '%s:%s' % (self.user, self.passwd)
        if credentials == ':':
            url = 'ftp://%s%s' % (self.host.rstrip('/'), abspath(path))
        else:
            url = 'ftp://%s@%s%s' % (credentials, self.host.rstrip('/'), abspath(path))
        return url

    @iotools.filelike_to_stream
    @ftperrors
    def open(self, path, mode, buffering=-1, encoding=None, errors=None, newline=None, line_buffering=False, **kwargs):
        path = normpath(path)
        mode = mode.lower()
        if self.isdir(path):
            raise ResourceInvalidError(path)
        if 'r' in mode or 'a' in mode:
            if not self.isfile(path):
                raise ResourceNotFoundError(path)
        if 'w' in mode or 'a' in mode or '+' in mode:
            self.refresh_dircache(dirname(path))
        ftp = self._open_ftp()
        f = _FTPFile(self, ftp, normpath(path), mode)
        return f

    @ftperrors
    def setcontents(self, path, data=b'', encoding=None, errors=None, chunk_size=1024*64):
        path = normpath(path)
        data = iotools.make_bytes_io(data, encoding=encoding, errors=errors)
        self.refresh_dircache(dirname(path))
        self.ftp.storbinary('STOR %s' % _encode(path), data, blocksize=chunk_size)

    @ftperrors
    def getcontents(self, path, mode="rb", encoding=None, errors=None, newline=None):
        path = normpath(path)
        contents = StringIO()
        self.ftp.retrbinary('RETR %s' % _encode(path), contents.write, blocksize=1024*64)
        data = contents.getvalue()
        if 'b' in data:
            return data
        return iotools.decode_binary(data, encoding=encoding, errors=errors)

    @ftperrors
    def exists(self, path):
        path = normpath(path)
        if path in ('', '/'):
            return True
        dirlist, fname = self._get_dirlist(path)
        return fname in dirlist

    @ftperrors
    def isdir(self, path):
        path = normpath(path)
        if path in ('', '/'):
            return True
        dirlist, fname = self._get_dirlist(path)
        info = dirlist.get(fname)
        if info is None:
            return False
        return info['try_cwd']

    @ftperrors
    def isfile(self, path):
        path = normpath(path)
        if path in ('', '/'):
            return False
        dirlist, fname = self._get_dirlist(path)
        info = dirlist.get(fname)
        if info is None:
            return False
        return not info['try_cwd']

    @ftperrors
    def listdir(self, path="./", wildcard=None, full=False, absolute=False, dirs_only=False, files_only=False):
        path = normpath(path)
        #self.clear_dircache(path)
        if not self.exists(path):
            raise ResourceNotFoundError(path)
        if not self.isdir(path):
            raise ResourceInvalidError(path)
        paths = self._readdir(path).keys()

        return self._listdir_helper(path, paths, wildcard, full, absolute, dirs_only, files_only)

    @ftperrors
    def listdirinfo(self, path="./",
                          wildcard=None,
                          full=False,
                          absolute=False,
                          dirs_only=False,
                          files_only=False):
        path = normpath(path)
        def getinfo(p):
            try:
                if full or absolute:
                    return self.getinfo(p)
                else:
                    return self.getinfo(pathjoin(path,p))
            except FSError:
                return {}

        return [(p, getinfo(p))
                    for p in self.listdir(path,
                                          wildcard=wildcard,
                                          full=full,
                                          absolute=absolute,
                                          dirs_only=dirs_only,
                                          files_only=files_only)]

    @ftperrors
    def makedir(self, path, recursive=False, allow_recreate=False):
        path = normpath(path)
        if path in ('', '/'):
            return
        def checkdir(path):
            if not self.isdir(path):
                self.clear_dircache(dirname(path))
                try:
                    self.ftp.mkd(_encode(path))
                except error_reply:
                    return
                except error_perm, e:
                    if recursive or allow_recreate:
                        return
                    if str(e).split(' ', 1)[0]=='550':
                        raise DestinationExistsError(path)
                    else:
                        raise
        if recursive:
            for p in recursepath(path):
                checkdir(p)
        else:
            base = dirname(path)
            if not self.exists(base):
                raise ParentDirectoryMissingError(path)

            if not allow_recreate:
                if self.exists(path):
                    if self.isfile(path):
                        raise ResourceInvalidError(path)
                    raise DestinationExistsError(path)
            checkdir(path)

    @ftperrors
    def remove(self, path):
        if not self.exists(path):
            raise ResourceNotFoundError(path)
        if not self.isfile(path):
            raise ResourceInvalidError(path)
        self.refresh_dircache(dirname(path))
        self.ftp.delete(_encode(path))

    @ftperrors
    def removedir(self, path, recursive=False, force=False):
        path = abspath(normpath(path))
        if not self.exists(path):
            raise ResourceNotFoundError(path)
        if self.isfile(path):
            raise ResourceInvalidError(path)
        if normpath(path) in ('', '/'):
            raise RemoveRootError(path)

        if not force:
            for _checkpath in self.listdir(path):
                raise DirectoryNotEmptyError(path)
        try:
            if force:
                for rpath in self.listdir(path, full=True):
                    try:
                        if self.isfile(rpath):
                            self.remove(rpath)
                        elif self.isdir(rpath):
                            self.removedir(rpath, force=force)
                    except FSError:
                        pass
            self.clear_dircache(dirname(path))
            self.ftp.rmd(_encode(path))
        except error_reply:
            pass
        if recursive:
            try:
                if dirname(path) not in ('', '/'):
                    self.removedir(dirname(path), recursive=True)
            except DirectoryNotEmptyError:
                pass
        self.clear_dircache(dirname(path), path)

    @ftperrors
    def rename(self, src, dst):
        try:
            self.refresh_dircache(dirname(src), dirname(dst))
            self.ftp.rename(_encode(src), _encode(dst))
        except error_perm, exception:
            code, message = str(exception).split(' ', 1)
            if code == "550":
                if not self.exists(dirname(dst)):
                    raise ParentDirectoryMissingError(dst)
            raise
        except error_reply:
            pass

    @ftperrors
    def getinfo(self, path):
        dirlist, fname = self._check_path(path)
        if not fname:
            return {}
        info = dirlist[fname].copy()
        info['modified_time'] = datetime.datetime.fromtimestamp(info['mtime'])
        info['created_time'] = info['modified_time']
        return info

    @ftperrors
    def getsize(self, path):

        size = None
        if self.dircache.count:
            dirlist, fname = self._check_path(path)
            size = dirlist[fname].get('size')

        if size is not None:
            return size

        self.ftp.sendcmd('TYPE I')
        size = self.ftp.size(_encode(path))
        if size is None:
            dirlist, fname = self._check_path(path)
            size = dirlist[fname].get('size')
        if size is None:
            raise OperationFailedError('getsize', path)
        return size

    @ftperrors
    def desc(self, path):
        path = normpath(path)
        url = self.getpathurl(path, allow_none=True)
        if url:
            return url
        dirlist, fname = self._check_path(path)
        if fname not in dirlist:
            raise ResourceNotFoundError(path)
        return dirlist[fname].get('raw_line', 'No description available')

    @ftperrors
    def move(self, src, dst, overwrite=False, chunk_size=16384):
        if not overwrite and self.exists(dst):
            raise DestinationExistsError(dst)
        #self.refresh_dircache(dirname(src), dirname(dst))
        try:
            self.rename(src, dst)
        except:
            self.copy(src, dst, overwrite=overwrite)
            self.remove(src)
        finally:
            self.refresh_dircache(src, dirname(src), dst, dirname(dst))

    @ftperrors
    def copy(self, src, dst, overwrite=False, chunk_size=1024*64):
        if not self.isfile(src):
            if self.isdir(src):
                raise ResourceInvalidError(src, msg="Source is not a file: %(path)s")
            raise ResourceNotFoundError(src)
        if not overwrite and self.exists(dst):
            raise DestinationExistsError(dst)

        dst = normpath(dst)
        src_file = None
        try:
            src_file = self.open(src, "rb")
            ftp = self._open_ftp()
            ftp.voidcmd('TYPE I')
            ftp.storbinary('STOR %s' % _encode(normpath(dst)), src_file, blocksize=chunk_size)
        finally:
            self.refresh_dircache(dirname(dst))
            if src_file is not None:
                src_file.close()


    @ftperrors
    def movedir(self, src, dst, overwrite=False, ignore_errors=False, chunk_size=16384):
        self.clear_dircache(dirname(src), dirname(dst))
        super(FTPFS, self).movedir(src, dst, overwrite, ignore_errors, chunk_size)

    @ftperrors
    def copydir(self, src, dst, overwrite=False, ignore_errors=False, chunk_size=16384):
        self.clear_dircache(dirname(dst))
        super(FTPFS, self).copydir(src, dst, overwrite, ignore_errors, chunk_size)


if __name__ == "__main__":

    ftp_fs = FTPFS('ftp.ncsa.uiuc.edu')
    ftp_fs.cache_hint(True)
    from fs.browsewin import browse
    browse(ftp_fs)

    #ftp_fs = FTPFS('127.0.0.1', 'user', '12345', dircache=True)
    #f = ftp_fs.open('testout.txt', 'w')
    #f.write("Testing writing to an ftp file!")
    #f.write("\nHai!")
    #f.close()

    #ftp_fs.createfile(u"\N{GREEK CAPITAL LETTER KAPPA}", 'unicode!')

    #kappa = u"\N{GREEK CAPITAL LETTER KAPPA}"
    #ftp_fs.makedir(kappa)

    #print repr(ftp_fs.listdir())

    #print repr(ftp_fs.listdir())

    #ftp_fs.makedir('a/b/c/d', recursive=True)
    #print ftp_fs.getsize('/testout.txt')


    #print f.read()
    #for p in ftp_fs:
    #    print p

    #from fs.utils import print_fs
    #print_fs(ftp_fs)

    #print ftp_fs.getsize('test.txt')

    #from fs.browsewin import browse
    #browse(ftp_fs)
