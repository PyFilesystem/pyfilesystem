#  Copyright (c) 2009-2010, Cloud Matrix Pty. Ltd.
#  All rights reserved; available under the terms of the MIT License.
"""

  fs.expose.dokan.libdokan:  low-level ctypes interface to Dokan

"""

from ctypes import *

try:
    DokanMain = windll.Dokan.DokanMain
    DokanVersion = windll.Dokan.DokanVersion
except AttributeError:
    raise ImportError("Dokan DLL not found")


from ctypes.wintypes import *
ULONG64 = c_ulonglong
ULONGLONG = c_ulonglong
PULONGLONG = POINTER(ULONGLONG)
UCHAR = c_ubyte
LPDWORD = POINTER(DWORD)
LONGLONG = c_longlong

try:
    USHORT = USHORT
except NameError:
    #  Not available in older python versions
    USHORT = c_ushort


DokanVersion.restype = ULONG
DokanVersion.argtypes = ()
if DokanVersion() < 392:  # ths is release 0.5.3
    raise ImportError("Dokan DLL is too old")


MAX_PATH = 260

class FILETIME(Structure):
    _fields_ = [
        ("dwLowDateTime", DWORD),
        ("dwHighDateTime", DWORD),
    ]

class WIN32_FIND_DATAW(Structure):
    _fields_ = [
        ("dwFileAttributes", DWORD),
        ("ftCreationTime", FILETIME),
        ("ftLastAccessTime", FILETIME),
        ("ftLastWriteTime", FILETIME),
        ("nFileSizeHigh", DWORD),
        ("nFileSizeLow", DWORD),
        ("dwReserved0", DWORD),
        ("dwReserved1", DWORD),
        ("cFileName", WCHAR * MAX_PATH),
        ("cAlternateFileName", WCHAR * 14),
    ]

class BY_HANDLE_FILE_INFORMATION(Structure):
    _fields_ = [
        ('dwFileAttributes', DWORD),
        ('ftCreationTime', FILETIME),
        ('ftLastAccessTime', FILETIME),
        ('ftLastWriteTime', FILETIME),
        ('dwVolumeSerialNumber', DWORD),
        ('nFileSizeHigh', DWORD),
        ('nFileSizeLow', DWORD),
        ('nNumberOfLinks', DWORD),
        ('nFileIndexHigh', DWORD),
        ('nFileIndexLow', DWORD),
    ]

class DOKAN_OPTIONS(Structure):
    _fields_ = [
	("DriveLetter", WCHAR),
	("ThreadCount", USHORT),
	("Options", ULONG),
	("GlobalContext", ULONG64),
    ]


class DOKAN_FILE_INFO(Structure):
    _fields_ = [
        ("Context", ULONG64),
        ("DokanContext", ULONG64),
        ("DokanOptions", POINTER(DOKAN_OPTIONS)),
        ("ProcessId", ULONG),
        ("IsDirectory", UCHAR),
        ("DeleteOnClose", UCHAR),
        ("PagingIO", UCHAR),
        ("SyncronousIo", UCHAR),
        ("Nocache", UCHAR),
        ("WriteToEndOfFile", UCHAR),
]


PDOKAN_FILE_INFO = POINTER(DOKAN_FILE_INFO)
PFillFindData = WINFUNCTYPE(c_int,POINTER(WIN32_FIND_DATAW),PDOKAN_FILE_INFO)

class DOKAN_OPERATIONS(Structure):
    _fields_ = [
        ("CreateFile", CFUNCTYPE(c_int,
                LPCWSTR,      # FileName
                DWORD,        # DesiredAccess
                DWORD,        # ShareMode
                DWORD,        # CreationDisposition
                DWORD,        # FlagsAndAttributes
                PDOKAN_FILE_INFO)),
        ("OpenDirectory", CFUNCTYPE(c_int,
		LPCWSTR,      # FileName
		PDOKAN_FILE_INFO)),
        ("CreateDirectory", CFUNCTYPE(c_int,
		LPCWSTR,      # FileName
		PDOKAN_FILE_INFO)),
        ("Cleanup", CFUNCTYPE(c_int,
		LPCWSTR,      # FileName
		PDOKAN_FILE_INFO)),
        ("CloseFile", CFUNCTYPE(c_int,
		LPCWSTR,      # FileName
		PDOKAN_FILE_INFO)),
        ("ReadFile", CFUNCTYPE(c_int,
		LPCWSTR,  # FileName
		POINTER(c_char),   # Buffer
		DWORD,    # NumberOfBytesToRead
		LPDWORD,  # NumberOfBytesRead
		LONGLONG, # Offset
		PDOKAN_FILE_INFO)),
        ("WriteFile", CFUNCTYPE(c_int,
		LPCWSTR,  # FileName
		POINTER(c_char), # Buffer
		DWORD,    # NumberOfBytesToWrite
		LPDWORD,  # NumberOfBytesWritten
		LONGLONG, # Offset
		PDOKAN_FILE_INFO)),
        ("FlushFileBuffers", CFUNCTYPE(c_int,
		LPCWSTR,  # FileName
		PDOKAN_FILE_INFO)),
        ("GetFileInformation", CFUNCTYPE(c_int,
		LPCWSTR,  # FileName
		POINTER(BY_HANDLE_FILE_INFORMATION), # Buffer
		PDOKAN_FILE_INFO)),
        ("FindFiles", CFUNCTYPE(c_int,
		LPCWSTR,  # PathName
		PFillFindData, # call this function with PWIN32_FIND_DATAW
		PDOKAN_FILE_INFO)),
        ("FindFilesWithPattern", CFUNCTYPE(c_int,
		LPCWSTR,  # PathName
		LPCWSTR,  # SearchPattern
		PFillFindData,	#call this function with PWIN32_FIND_DATAW
		PDOKAN_FILE_INFO)),
        ("SetFileAttributes", CFUNCTYPE(c_int,
		LPCWSTR, # FileName
		DWORD,   # FileAttributes
		PDOKAN_FILE_INFO)),
        ("SetFileTime", CFUNCTYPE(c_int,
		LPCWSTR, # FileName
		POINTER(FILETIME), # CreationTime
		POINTER(FILETIME), # LastAccessTime
		POINTER(FILETIME), # LastWriteTime
		PDOKAN_FILE_INFO)),
        ("DeleteFile", CFUNCTYPE(c_int,
		LPCWSTR, # FileName
		PDOKAN_FILE_INFO)),
        ("DeleteDirectory", CFUNCTYPE(c_int,
		LPCWSTR, # FileName
		PDOKAN_FILE_INFO)),
        ("MoveFile", CFUNCTYPE(c_int,
		LPCWSTR, # ExistingFileName
		LPCWSTR, # NewFileName
		BOOL,	 # ReplaceExisiting
		PDOKAN_FILE_INFO)),
        ("SetEndOfFile", CFUNCTYPE(c_int,
		LPCWSTR,  # FileName
		LONGLONG, # Length
		PDOKAN_FILE_INFO)),
        ("SetAllocationSize", CFUNCTYPE(c_int,
		LPCWSTR,  # FileName
		LONGLONG, # Length
		PDOKAN_FILE_INFO)),
        ("LockFile", CFUNCTYPE(c_int,
		LPCWSTR,  # FileName
		LONGLONG, # ByteOffset
		LONGLONG, # Length
		PDOKAN_FILE_INFO)),
        ("UnlockFile", CFUNCTYPE(c_int,
		LPCWSTR,  # FileName
		LONGLONG, # ByteOffset
		LONGLONG, # Length
		PDOKAN_FILE_INFO)),
        ("GetDiskFreeSpaceEx", CFUNCTYPE(c_int,
		PULONGLONG, # FreeBytesAvailable
		PULONGLONG, # TotalNumberOfBytes
		PULONGLONG, # TotalNumberOfFreeBytes
		PDOKAN_FILE_INFO)),
        ("GetVolumeInformation", CFUNCTYPE(c_int,
		POINTER(c_wchar),  # VolumeNameBuffer
		DWORD,	 # VolumeNameSize in num of chars
		LPDWORD, # VolumeSerialNumber
		LPDWORD, # MaximumComponentLength in num of chars
		LPDWORD, # FileSystemFlags
		POINTER(c_wchar),  # FileSystemNameBuffer
		DWORD,	 # FileSystemNameSize in num of chars
		PDOKAN_FILE_INFO)),
        ("Unmount", CFUNCTYPE(c_int,
		PDOKAN_FILE_INFO)),
    ]



DokanMain.restype = c_int
DokanMain.argtypes = (
    POINTER(DOKAN_OPTIONS),
    POINTER(DOKAN_OPERATIONS),
)



DokanUnmount = windll.Dokan.DokanUnmount
DokanUnmount.restype = BOOL
DokanUnmount.argtypes = (
    WCHAR,
)

DokanIsNameInExpression = windll.Dokan.DokanIsNameInExpression
DokanIsNameInExpression.restype = BOOL
DokanIsNameInExpression.argtypes = (
    LPCWSTR,  # pattern
    LPCWSTR,  # name
    BOOL,     # ignore case
)

DokanDriverVersion = windll.Dokan.DokanDriverVersion
DokanDriverVersion.restype = ULONG
DokanDriverVersion.argtypes = (
)

DokanResetTimeout = windll.Dokan.DokanResetTimeout
DokanResetTimeout.restype = BOOL
DokanResetTimeout.argtypes = (
    ULONG,  #timeout
    PDOKAN_FILE_INFO,  # file info pointer
)


