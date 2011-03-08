Command Line Applications
=========================

PyFilesystem adds a number of applications that expose some of the PyFilesystem functionality to the command line.
These commands use the opener syntax, as described in :doc:`opening`, to refer to filesystems.

Most of these applications shadow existing shell commands and work in similar ways.

All of the command line application support the `-h` (or `--help`) switch which will display a full list of options.


Custom Filesystem Openers
-------------------------

When opening filesystems, the command line applications will use the default openers.
You can also 'point' the command line applications at an opener to add it to a list of available openers.
For example, the following uses a custom opener to list the contents of a directory served with the 'myfs' protocol::

	fsls --fs mypackage.mymodule.myfs.MyFSOpener myfs://127.0.0.1
	
	
Listing Supported Filesystems
-----------------------------

All of the command line applications support the ``--listopeners`` switch, which lists all available installed openers::

	fsls --listopeners


fsls
----

Lists the contents of a directory, similar to the ``ls`` command, e.g.::

	fsls	
	fsls ../
	fsls ftp://example.org/pub
	fsls zip://photos.zip

fstree
------

Displays an ASCII tree of a directory. e.g::

	fstree
	fstree -g
	fstree rpc://192.168.1.64/foo/bar -l3
	fstree zip://photos.zip

fscat
-----

Writes a file to stdout, e.g::

	fscat ~/.bashrc
	fscat http://www.willmcgugan.com
	fscat ftp://ftp.mozilla.org/pub/README
	
fsinfo
------

Displays information regarding a file / directory, e.g::

	fsinfo C:\autoexec.bat
	fsinfo ftp://ftp.mozilla.org/pub/README
	
fsmv
----

Moves a file from one location to another, e.g::

	fsmv foo bar
	fsmv *.jpg zip://photos.zip
	
fsmkdir
-------

Makes a directory on a filesystem, e.g::

	fsmkdir foo
	fsmkdir ftp://ftp.mozilla.org/foo
	fsmkdir rpc://127.0.0.1/foo

fscp
----

Copies a file from one location to another, e.g::

	fscp foo bar
	fscp ftp://ftp.mozilla.org/pub/README readme.txt

fsrm
----

Removes (deletes) a file from a filesystem, e.g::

	fsrm foo
	fsrm -r mydir

fsserve
-------

Serves the contents of a filesystem over a network with one of a number of methods; HTTP, RPC or SFTP, e.g:: 

	fsserve
	fsserve --type rpc
	fsserve --type http zip://photos.zip	

fsmount
-------

Mounts a filesystem with FUSE (on Linux) and Dokan (on Windows), e.g::

	fsmount mem:// ram
	fsserve mem:// M
	fsserve ftp://ftp.mozilla.org/pub ftpgateway