#  Copyright (c) 2009-2010, Cloud Matrix Pty. Ltd.
#  Copyright (c) 2016-2016, Adrien J. <liryna.stark@gmail.com>.
#  All rights reserved; available under the terms of the MIT License.
"""

  fs.expose.dokan.libdokan:  low-level ctypes interface to Dokan

"""

import ctypes

try:
    DokanMain = ctypes.windll.Dokan1.DokanMain
    DokanVersion = ctypes.windll.Dokan1.DokanVersion
except AttributeError:
    raise ImportError("Dokan DLL not found")


from ctypes.wintypes import *
ULONG64 = ctypes.c_ulonglong
PULONGLONG = ctypes.POINTER(ctypes.c_ulonglong)
PVOID = ctypes.c_void_p
PULONG = ctypes.POINTER(ctypes.c_ulong)
UCHAR = ctypes.c_ubyte
LPDWORD = ctypes.POINTER(ctypes.c_ulong)
LONGLONG = ctypes.c_longlong
NTSTATUS = ctypes.c_long
USHORT = ctypes.c_ushort
WCHAR = ctypes.c_wchar


DokanVersion.restype = ULONG
DokanVersion.argtypes = ()
DOKAN_MINIMUM_COMPATIBLE_VERSION = 100  # this is release 1.0.0
if DokanVersion() < DOKAN_MINIMUM_COMPATIBLE_VERSION:
    raise ImportError("Dokan DLL is too old")


MAX_PATH = 260

class SECURITY_DESCRIPTOR(ctypes.Structure): pass

PSECURITY_DESCRIPTOR = ctypes.POINTER(SECURITY_DESCRIPTOR)
PPSECURITY_DESCRIPTOR = ctypes.POINTER(PSECURITY_DESCRIPTOR)

SECURITY_INFORMATION = DWORD
PSECURITY_INFORMATION = ctypes.POINTER(SECURITY_INFORMATION)

class FILETIME(ctypes.Structure):
    _fields_ = [
        ("dwLowDateTime", DWORD),
        ("dwHighDateTime", DWORD),
    ]


class WIN32_FIND_DATAW(ctypes.Structure):
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


class BY_HANDLE_FILE_INFORMATION(ctypes.Structure):
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


class DOKAN_OPTIONS(ctypes.Structure):
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


class DOKAN_FILE_INFO(ctypes.Structure):
    _fields_ = [
        ("Context", ULONG64),
        ("DokanContext", ULONG64),
        ("DokanOptions", ctypes.POINTER(DOKAN_OPTIONS)),
        ("ProcessId", ULONG),
        ("IsDirectory", UCHAR),
        ("DeleteOnClose", UCHAR),
        ("PagingIO", UCHAR),
        ("SyncronousIo", UCHAR),
        ("Nocache", UCHAR),
        ("WriteToEndOfFile", UCHAR),
    ]


PDOKAN_FILE_INFO = ctypes.POINTER(DOKAN_FILE_INFO)
PFillFindData = ctypes.WINFUNCTYPE(ctypes.c_int, ctypes.POINTER(WIN32_FIND_DATAW), PDOKAN_FILE_INFO)


class DOKAN_OPERATIONS(ctypes.Structure):
    _fields_ = [
        ("ZwCreateFile", ctypes.WINFUNCTYPE(NTSTATUS,
        LPCWSTR,      # FileName
        PVOID,        # SecurityContext, see
                      # https://msdn.microsoft.com/en-us/library/windows/hardware/ff550613(v=vs.85).aspx
        DWORD,        # DesiredAccess
        ULONG,        # FileAttributes
        ULONG,        # ShareAccess
        ULONG,        # CreateDisposition
        ULONG,        # CreateOptions
        PDOKAN_FILE_INFO)),
        ("Cleanup", ctypes.WINFUNCTYPE(None,
        LPCWSTR,      # FileName
        PDOKAN_FILE_INFO)),
        ("CloseFile", ctypes.WINFUNCTYPE(None,
        LPCWSTR,      # FileName
        PDOKAN_FILE_INFO)),
        ("ReadFile", ctypes.WINFUNCTYPE(NTSTATUS,
        LPCWSTR,  # FileName
        LPVOID,   # Buffer
        DWORD,    # NumberOfBytesToRead
        LPDWORD,  # NumberOfBytesRead
        LONGLONG, # Offset
        PDOKAN_FILE_INFO)),
        ("WriteFile", ctypes.WINFUNCTYPE(NTSTATUS,
        LPCWSTR,  # FileName
        LPCVOID,  # Buffer
        DWORD,    # NumberOfBytesToWrite
        LPDWORD,  # NumberOfBytesWritten
        LONGLONG, # Offset
        PDOKAN_FILE_INFO)),
        ("FlushFileBuffers", ctypes.WINFUNCTYPE(NTSTATUS,
        LPCWSTR,  # FileName
        PDOKAN_FILE_INFO)),
        ("GetFileInformation", ctypes.WINFUNCTYPE(NTSTATUS,
        LPCWSTR,  # FileName
        ctypes.POINTER(BY_HANDLE_FILE_INFORMATION), # Buffer
        PDOKAN_FILE_INFO)),
        ("FindFiles", ctypes.WINFUNCTYPE(NTSTATUS,
        LPCWSTR,  # PathName
        PFillFindData, # call this function with PWIN32_FIND_DATAW
        PDOKAN_FILE_INFO)),
        ("FindFilesWithPattern", ctypes.WINFUNCTYPE(NTSTATUS,
        LPCWSTR,  # PathName
        LPCWSTR,  # SearchPattern
        PFillFindData,  #call this function with PWIN32_FIND_DATAW
        PDOKAN_FILE_INFO)),
        ("SetFileAttributes", ctypes.WINFUNCTYPE(NTSTATUS,
        LPCWSTR, # FileName
        DWORD,   # FileAttributes
        PDOKAN_FILE_INFO)),
        ("SetFileTime", ctypes.WINFUNCTYPE(NTSTATUS,
        LPCWSTR, # FileName
        ctypes.POINTER(FILETIME), # CreationTime
        ctypes.POINTER(FILETIME), # LastAccessTime
        ctypes.POINTER(FILETIME), # LastWriteTime
        PDOKAN_FILE_INFO)),
        ("DeleteFile", ctypes.WINFUNCTYPE(NTSTATUS,
        LPCWSTR, # FileName
        PDOKAN_FILE_INFO)),
        ("DeleteDirectory", ctypes.WINFUNCTYPE(NTSTATUS,
        LPCWSTR, # FileName
        PDOKAN_FILE_INFO)),
        ("MoveFile", ctypes.WINFUNCTYPE(NTSTATUS,
        LPCWSTR, # ExistingFileName
        LPCWSTR, # NewFileName
        BOOL,    # ReplaceExisiting
        PDOKAN_FILE_INFO)),
        ("SetEndOfFile", ctypes.WINFUNCTYPE(NTSTATUS,
        LPCWSTR,  # FileName
        LONGLONG, # Length
        PDOKAN_FILE_INFO)),
        ("SetAllocationSize", ctypes.WINFUNCTYPE(NTSTATUS,
        LPCWSTR,  # FileName
        LONGLONG, # Length
        PDOKAN_FILE_INFO)),
        ("LockFile", ctypes.WINFUNCTYPE(NTSTATUS,
        LPCWSTR,  # FileName
        LONGLONG, # ByteOffset
        LONGLONG, # Length
        PDOKAN_FILE_INFO)),
        ("UnlockFile", ctypes.WINFUNCTYPE(NTSTATUS,
        LPCWSTR,  # FileName
        LONGLONG, # ByteOffset
        LONGLONG, # Length
        PDOKAN_FILE_INFO)),
        ("GetDiskFreeSpace", ctypes.WINFUNCTYPE(NTSTATUS,
        PULONGLONG, # FreeBytesAvailable
        PULONGLONG, # TotalNumberOfBytes
        PULONGLONG, # TotalNumberOfFreeBytes
        PDOKAN_FILE_INFO)),
        ("GetVolumeInformation", ctypes.WINFUNCTYPE(NTSTATUS,
        PVOID,  # VolumeNameBuffer
        DWORD,   # VolumeNameSize in num of chars
        LPDWORD, # VolumeSerialNumber
        LPDWORD, # MaximumComponentLength in num of chars
        LPDWORD, # FileSystemFlags
        PVOID,  # FileSystemNameBuffer
        DWORD,   # FileSystemNameSize in num of chars
        PDOKAN_FILE_INFO)),
        ("Mounted", ctypes.WINFUNCTYPE(NTSTATUS,
        PDOKAN_FILE_INFO)),
        ("Unmounted", ctypes.WINFUNCTYPE(NTSTATUS,
        DOKAN_FILE_INFO)),
        ("GetFileSecurity", ctypes.WINFUNCTYPE(NTSTATUS,
        LPCWSTR, # FileName
        PULONG,   # A pointer to SECURITY_INFORMATION value being requested
        PVOID,   # A pointer to SECURITY_DESCRIPTOR buffer to be filled
        ULONG,   # Length of Security descriptor buffer
        PULONG,  # Length Needed
        PDOKAN_FILE_INFO)),
        ("SetFileSecurity", ctypes.WINFUNCTYPE(NTSTATUS,
        LPCWSTR, # FileName
        PVOID,   # A pointer to SECURITY_INFORMATION value being
        PVOID,   # A pointer to SECURITY_DESCRIPTOR buffer
        ULONG,   # Length of Security descriptor buffer
        PDOKAN_FILE_INFO)),
        ("FindStreams", ctypes.WINFUNCTYPE(NTSTATUS,
        LPCWSTR, # FileName
        PVOID,   # call this function with PWIN32_FIND_STREAM_DATA
        PDOKAN_FILE_INFO))
    ]


DokanMain.restype = ctypes.c_int
DokanMain.argtypes = (
    ctypes.POINTER(DOKAN_OPTIONS),
    ctypes.POINTER(DOKAN_OPERATIONS),
)

DokanRemoveMountPoint = ctypes.windll.Dokan1.DokanRemoveMountPoint
DokanRemoveMountPoint.restype = BOOL
DokanRemoveMountPoint.argtypes = (
    LPCWSTR,
)

DokanIsNameInExpression = ctypes.windll.Dokan1.DokanIsNameInExpression
DokanIsNameInExpression.restype = BOOL
DokanIsNameInExpression.argtypes = (
    LPCWSTR,  # pattern
    LPCWSTR,  # name
    BOOL,     # ignore case
)

DokanDriverVersion = ctypes.windll.Dokan1.DokanDriverVersion
DokanDriverVersion.restype = ULONG
DokanDriverVersion.argtypes = (
)

DokanResetTimeout = ctypes.windll.Dokan1.DokanResetTimeout
DokanResetTimeout.restype = BOOL
DokanResetTimeout.argtypes = (
    ULONG,  #timeout
    PDOKAN_FILE_INFO,  # file info pointer
)

GetFileSecurity = ctypes.windll.advapi32.GetFileSecurityW
GetFileSecurity.restype = BOOL
GetFileSecurity.argtypes = (
    LPWSTR,                     # _In_ LPCTSTR lpFileName,
    SECURITY_INFORMATION,       # _In_ SECURITY_INFORMATION RequestedInformation,
    PSECURITY_DESCRIPTOR,       # _Out_opt_ PSECURITY_DESCRIPTOR pSecurityDescriptor,
    DWORD,                      # _In_ DWORD nLength,
    LPDWORD,                    # _Out_ LPDWORD lpnLengthNeeded
)
