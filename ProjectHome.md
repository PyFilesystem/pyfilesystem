# Pyfilesystem #

Pyfilesystem is a Python module that provides a _simplified_ common interface to many types of filesystem. Filesystems exposed via Pyfilesystem can also be served over the network, or 'mounted' on the native filesystem.

Pyfilesystem simplifies working directories and paths, even if you only intend to work with local files. Differences in path formats between platforms are abstracted away, and you can write code that _sand-boxes_ any changes to a given directory.

Pyfilesystem works with Linux, Windows and Mac.

## Suported Filesystems ##
Here are a few of the filesystems that can be accessed with Pyfilesystem:

| **DavFS** | access files & directories on a WebDAV server |
|:----------|:----------------------------------------------|
| **FTPFS** | access files & directories on an FTP server |
| **MemoryFS** | access files & directories stored in memory (non-permanent but very fast) |
| **MountFS** | creates a virtual directory structure built from other filesystems |
| **MultiFS** | a virtual filesystem that combines a list of filesystems in to one, and checks them in order when opening files |
| **OSFS** | the native filesystem |
| **SFTPFS** | access files & directires stored on a Secure FTP server |
| **S3FS** | access files & directories stored on Amazon S3 storage |
| **TahoeLAFS** | access files & directories stored on a Tahoe distributed filesystem |
| **ZipFS** | access files and directories contained in a zip file |

## Example ##

The following snippet prints the total number of bytes contained in all your Python files in `C:/projects` (including sub-directories).
```
    from fs.osfs import OSFS
    projects_fs = OSFS('C:/projects')
    print sum(projects_fs.getsize(path)
              for path in projects_fs.walkfiles(wildcard="*.py"))
```
That is, assuming you are on Windows and have a directory called 'projects' in your C drive. If you are on Linux / Mac, you might replace the second line with something like:
```
    projects_fs = OSFS('~/projects')
```
If you later want to display the total size of Python files stored in a zip file, you could make the following change to the first two lines:
```
    from fs.zipfs import ZipFS
    projects_fs = ZipFS('source.zip')
```
In fact, you could use any of the supported filesystems above, and the code would continue to work as before.

An alternative to explicity importing the filesystem class you want, is to use an FS opener which opens a filesystem from a URL-like syntax:
```
    from fs.opener import fsopendir
    projects_fs = fsopendir('C:/projects')
```
You could change `'C:/projects'` to `'zip://source.zip'` to open the zip file, or even `'ftp://ftp.example.org/code/projects/'` to sum up the bytes of Python stored on an ftp server.

## Screencast ##

http://vimeo.com/12680842

## Documentation ##

http://docs.pyfilesystem.org/

## Discussion Group ##

http://groups.google.com/group/pyfilesystem-discussion