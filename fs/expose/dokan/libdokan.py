#  Copyright (c) 2009-2010, Cloud Matrix Pty. Ltd.
#  Copyright (c) 2016-2016, Adrien J. <liryna.stark@gmail.com>.
#  All rights reserved; available under the terms of the MIT License.
"""

  fs.expose.dokan.libdokan:  low-level ctypes interface to Dokan

"""

from ctypes import *

try:
    DokanMain = windll.Dokan1.DokanMain
    DokanVersion = windll.Dokan1.DokanVersion
except AttributeError:
    raise ImportError("Dokan DLL not found")


from ctypes.wintypes import *
ULONG64 = c_ulonglong
PULONGLONG = POINTER(c_ulonglong)
PVOID = c_void_p
PULONG = POINTER(c_ulong)
UCHAR = c_ubyte
LPDWORD = POINTER(c_ulong)
LONGLONG = c_longlong
NTSTATUS = c_long
USHORT = c_ushort
WCHAR = c_wchar


DokanVersion.restype = ULONG
DokanVersion.argtypes = ()
DOKAN_MINIMUM_COMPATIBLE_VERSION = 100  # this is release 1.0.0
if DokanVersion() < DOKAN_MINIMUM_COMPATIBLE_VERSION:
    raise ImportError("Dokan DLL is too old")


MAX_PATH = 260

class SECURITY_DESCRIPTOR(Structure): pass

PSECURITY_DESCRIPTOR = POINTER(SECURITY_DESCRIPTOR)
PPSECURITY_DESCRIPTOR = POINTER(PSECURITY_DESCRIPTOR)

SECURITY_INFORMATION = DWORD
PSECURITY_INFORMATION = POINTER(SECURITY_INFORMATION)

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
        ("Version", USHORT),
        ("ThreadCount", USHORT),
        ("Options", ULONG),
        ("GlobalContext", ULONG64),
        ("MountPoint", LPCWSTR),
        ("UNCName", LPCWSTR),
        ("Timeout", ULONG),
        ("AllocationUnitSize", ULONG),
        ("SectorSize", ULONG),
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
PFillFindData = WINFUNCTYPE(c_int, POINTER(WIN32_FIND_DATAW), PDOKAN_FILE_INFO)


class DOKAN_OPERATIONS(Structure):
    _fields_ = [
        ("ZwCreateFile", WINFUNCTYPE(NTSTATUS,
        LPCWSTR,      # FileName
        PVOID,        # SecurityContext, see
                      # https://msdn.microsoft.com/en-us/library/windows/hardware/ff550613(v=vs.85).aspx
        DWORD,        # DesiredAccess
        ULONG,        # FileAttributes
        ULONG,        # ShareAccess
        ULONG,        # CreateDisposition
        ULONG,        # CreateOptions
        PDOKAN_FILE_INFO)),
        ("Cleanup", WINFUNCTYPE(None,
        LPCWSTR,      # FileName
        PDOKAN_FILE_INFO)),
        ("CloseFile", WINFUNCTYPE(None,
        LPCWSTR,      # FileName
        PDOKAN_FILE_INFO)),
        ("ReadFile", WINFUNCTYPE(NTSTATUS,
        LPCWSTR,  # FileName
        LPVOID,   # Buffer
        DWORD,    # NumberOfBytesToRead
        LPDWORD,  # NumberOfBytesRead
        LONGLONG, # Offset
        PDOKAN_FILE_INFO)),
        ("WriteFile", WINFUNCTYPE(NTSTATUS,
        LPCWSTR,  # FileName
        LPCVOID,  # Buffer
        DWORD,    # NumberOfBytesToWrite
        LPDWORD,  # NumberOfBytesWritten
        LONGLONG, # Offset
        PDOKAN_FILE_INFO)),
        ("FlushFileBuffers", WINFUNCTYPE(NTSTATUS,
        LPCWSTR,  # FileName
        PDOKAN_FILE_INFO)),
        ("GetFileInformation", WINFUNCTYPE(NTSTATUS,
        LPCWSTR,  # FileName
        POINTER(BY_HANDLE_FILE_INFORMATION), # Buffer
        PDOKAN_FILE_INFO)),
        ("FindFiles", WINFUNCTYPE(NTSTATUS,
        LPCWSTR,  # PathName
        PFillFindData, # call this function with PWIN32_FIND_DATAW
        PDOKAN_FILE_INFO)),
        ("FindFilesWithPattern", WINFUNCTYPE(NTSTATUS,
        LPCWSTR,  # PathName
        LPCWSTR,  # SearchPattern
        PFillFindData,  #call this function with PWIN32_FIND_DATAW
        PDOKAN_FILE_INFO)),
        ("SetFileAttributes", WINFUNCTYPE(NTSTATUS,
        LPCWSTR, # FileName
        DWORD,   # FileAttributes
        PDOKAN_FILE_INFO)),
        ("SetFileTime", WINFUNCTYPE(NTSTATUS,
        LPCWSTR, # FileName
        POINTER(FILETIME), # CreationTime
        POINTER(FILETIME), # LastAccessTime
        POINTER(FILETIME), # LastWriteTime
        PDOKAN_FILE_INFO)),
        ("DeleteFile", WINFUNCTYPE(NTSTATUS,
        LPCWSTR, # FileName
        PDOKAN_FILE_INFO)),
        ("DeleteDirectory", WINFUNCTYPE(NTSTATUS,
        LPCWSTR, # FileName
        PDOKAN_FILE_INFO)),
        ("MoveFile", WINFUNCTYPE(NTSTATUS,
        LPCWSTR, # ExistingFileName
        LPCWSTR, # NewFileName
        BOOL,    # ReplaceExisiting
        PDOKAN_FILE_INFO)),
        ("SetEndOfFile", WINFUNCTYPE(NTSTATUS,
        LPCWSTR,  # FileName
        LONGLONG, # Length
        PDOKAN_FILE_INFO)),
        ("SetAllocationSize", WINFUNCTYPE(NTSTATUS,
        LPCWSTR,  # FileName
        LONGLONG, # Length
        PDOKAN_FILE_INFO)),
        ("LockFile", WINFUNCTYPE(NTSTATUS,
        LPCWSTR,  # FileName
        LONGLONG, # ByteOffset
        LONGLONG, # Length
        PDOKAN_FILE_INFO)),
        ("UnlockFile", WINFUNCTYPE(NTSTATUS,
        LPCWSTR,  # FileName
        LONGLONG, # ByteOffset
        LONGLONG, # Length
        PDOKAN_FILE_INFO)),
        ("GetDiskFreeSpace", WINFUNCTYPE(NTSTATUS,
        PULONGLONG, # FreeBytesAvailable
        PULONGLONG, # TotalNumberOfBytes
        PULONGLONG, # TotalNumberOfFreeBytes
        PDOKAN_FILE_INFO)),
        ("GetVolumeInformation", WINFUNCTYPE(NTSTATUS,
        PVOID,  # VolumeNameBuffer
        DWORD,   # VolumeNameSize in num of chars
        LPDWORD, # VolumeSerialNumber
        LPDWORD, # MaximumComponentLength in num of chars
        LPDWORD, # FileSystemFlags
        PVOID,  # FileSystemNameBuffer
        DWORD,   # FileSystemNameSize in num of chars
        PDOKAN_FILE_INFO)),
        ("Mounted", WINFUNCTYPE(NTSTATUS,
        PDOKAN_FILE_INFO)),
        ("Unmounted", WINFUNCTYPE(NTSTATUS,
        DOKAN_FILE_INFO)),
        ("GetFileSecurity", WINFUNCTYPE(NTSTATUS,
        LPCWSTR, # FileName
        PULONG,   # A pointer to SECURITY_INFORMATION value being requested
        PVOID,   # A pointer to SECURITY_DESCRIPTOR buffer to be filled
        ULONG,   # Length of Security descriptor buffer
        PULONG,  # Length Needed
        PDOKAN_FILE_INFO)),
        ("SetFileSecurity", WINFUNCTYPE(NTSTATUS,
        LPCWSTR, # FileName
        PVOID,   # A pointer to SECURITY_INFORMATION value being
        PVOID,   # A pointer to SECURITY_DESCRIPTOR buffer
        ULONG,   # Length of Security descriptor buffer
        PDOKAN_FILE_INFO)),
        ("FindStreams", WINFUNCTYPE(NTSTATUS,
        LPCWSTR, # FileName
        PVOID,   # call this function with PWIN32_FIND_STREAM_DATA
        PDOKAN_FILE_INFO))
    ]


DokanMain.restype = c_int
DokanMain.argtypes = (
    POINTER(DOKAN_OPTIONS),
    POINTER(DOKAN_OPERATIONS),
)

DokanRemoveMountPoint = windll.Dokan1.DokanRemoveMountPoint
DokanRemoveMountPoint.restype = BOOL
DokanRemoveMountPoint.argtypes = (
    LPCWSTR,
)

DokanIsNameInExpression = windll.Dokan1.DokanIsNameInExpression
DokanIsNameInExpression.restype = BOOL
DokanIsNameInExpression.argtypes = (
    LPCWSTR,  # pattern
    LPCWSTR,  # name
    BOOL,     # ignore case
)

DokanDriverVersion = windll.Dokan1.DokanDriverVersion
DokanDriverVersion.restype = ULONG
DokanDriverVersion.argtypes = (
)

DokanResetTimeout = windll.Dokan1.DokanResetTimeout
DokanResetTimeout.restype = BOOL
DokanResetTimeout.argtypes = (
    ULONG,  #timeout
    PDOKAN_FILE_INFO,  # file info pointer
)

GetFileSecurity = windll.advapi32.GetFileSecurityW
GetFileSecurity.restype = BOOL
GetFileSecurity.argtypes = (
    LPWSTR,                     # _In_ LPCTSTR lpFileName,
    SECURITY_INFORMATION,       # _In_ SECURITY_INFORMATION RequestedInformation,
    PSECURITY_DESCRIPTOR,       # _Out_opt_ PSECURITY_DESCRIPTOR pSecurityDescriptor,
    DWORD,                      # _In_ DWORD nLength,
    LPDWORD,                    # _Out_ LPDWORD lpnLengthNeeded
)
