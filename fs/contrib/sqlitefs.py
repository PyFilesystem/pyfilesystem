'''
Sqlite3 based file system using PyFileSystem

Developed and Contributed by Sensible Softwares Pvt. Ltd.
(http://bootstraptoday.com)

Contributed under the terms of the BSD License:
http://www.opensource.org/licenses/bsd-license.php
'''

import tempfile
import datetime

from fs.path import iteratepath, normpath,dirname,forcedir
from fs.path import frombase, basename,pathjoin
from fs.base import *
from fs.errors import *
from fs import _thread_synchronize_default
import apsw

def fetchone(cursor):
    '''
    return a single row from the cursor (equivalent to pysqlite fetchone function)
    '''
    row = None
    try:
        row = cursor.next()
    except:
        pass
    return(row)

def remove_end_slash(dirname):
    if dirname.endswith('/'):
        return dirname[:-1]
    return dirname

class SqliteFsFileBase(object):
    '''
    base class for representing the files in the sqlite file system
    '''
    def __init__(self, fs, path, id, real_file=None):
        assert(fs != None)
        assert(path != None)
        assert(id != None)
        self.fs = fs
        self.path = path
        self.id = id
        self.closed = False
        #real file like object. Most of the methods are passed to this object
        self.real_stream= real_file
        
    def close(self):
        if not self.closed and self.real_stream is not None:
            self._do_close()
            self.fs._on_close(self)
            self.real_stream.close()
            self.closed = True
        
    def __str__(self):
        return "<SqliteFS File in %s %s>" % (self.fs, self.path)

    __repr__ = __str__

    def __unicode__(self):
        return u"<SqliteFS File in %s %s>" % (self.fs, self.path)

    def __del__(self):
        if not self.closed:
            self.close()

    def flush(self):
        self.real_stream.flush()
        
    def __iter__(self):
        raise OperationFailedError('__iter__', self.path)
        
    def next(self):
        raise OperationFailedError('next', self.path)
        
    def readline(self, *args, **kwargs):
        raise OperationFailedError('readline', self.path)        
    
    def read(self, size=None):
        raise OperationFailedError('read', self.path)                

    def seek(self, *args, **kwargs):
        return self.real_stream.seek(*args, **kwargs)

    def tell(self):
        return self.real_stream.tell()

    def truncate(self, *args, **kwargs):
        raise OperationFailedError('truncate', self.path)
        
    def write(self, data):
        raise OperationFailedError('write', self.path)
        
    def writelines(self, *args, **kwargs):
        raise OperationFailedError('writelines', self.path)
        
    def __enter__(self):
        return self

    def __exit__(self,exc_type,exc_value,traceback):
        self.close()
        return False
    
class SqliteWritableFile(SqliteFsFileBase):
    '''
    represents an sqlite file. Usually used for 'writing'. OnClose will
    actually 'copy the contents from temp disk file to sqlite blob
    '''
    def __init__(self,fs, path, id):
        super(SqliteWritableFile, self).__init__(fs, path, id)
        #open a temp file and return that.
        self.real_stream = tempfile.SpooledTemporaryFile(max_size='128*1000')
        
    def _do_close(self):
        #push the contents of the file to blob
        self.fs._writeblob(self.id, self.real_stream)
        
    def truncate(self, *args, **kwargs):
        return self.real_stream.truncate(*args, **kwargs)
        
    def write(self, data):
        return self.real_stream.write(data)

    def writelines(self, *args, **kwargs):
        return self.real_stream.writelines(*args, **kwargs)
        
class SqliteReadableFile(SqliteFsFileBase):
    def __init__(self,fs, path, id, real_file):
        super(SqliteReadableFile, self).__init__(fs, path, id, real_file)
        assert(self.real_stream != None)        
        
    def _do_close(self):
        pass
    
    def __iter__(self):
        return iter(self.real_stream)

    def next(self):
        return self.real_stream.next()

    def readline(self, *args, **kwargs):
        return self.real_stream.readline(*args, **kwargs)
    
    def read(self, size=None):
        if( size==None):
            size=-1
        return self.real_stream.read(size)

            
class SqliteFS(FS):
    '''
    sqlite based file system to store the files in sqlite database as 'blobs'
    We need two tables one to store file or directory meta data
    another for storing the file contain. Two are seperate so that same file
    can be refered from multiple directories.
    FsFileMetaData table :
        id : file id
        name : name of file
        parent : id of parent directory for the file.
        
    FsDirMetaData table:
        name : name of the directory (wihtout parent directory names)
        fullpath : full path of the directory including the parent directory name
        parent_id : id of the parent directory
                
    FsFileTable:
        size : file size in bytes (this is actual file size). Blob size may be
           different if compressed
        type : file type (extension or mime type)
        compression : For future use, Initially None. Later it will define type
            of compression used to compress the file
        last_modified : timestamp of last modification
        author : who changed it last
        content : blob where actual file contents are stored.
        
    TODO : Need an open files table or a flag in sqlite database. To avoid
    opening the file twice. (even from the different process or thread)
    '''
    
    def __init__(self, sqlite_filename):
        super(SqliteFS, self).__init__()
        self.dbpath =sqlite_filename
        self.dbcon =None        
        self.__actual_query_cur = None
        self.__actual_update_cur =None
        self.open_files = []
        
    def close(self):
        '''
        unlock all files. and close all open connections.
        '''
        self.close_all()        
        self._closedb()
        super(SqliteFS,self).close()
        
    def _initdb(self):
        if( self.dbcon is None):
            self.dbcon = apsw.Connection(self.dbpath)        
            self._create_tables()
    
    @property
    def _querycur(self):
        assert(self.dbcon != None)
        if( self.__actual_query_cur == None):
            self.__actual_query_cur = self.dbcon.cursor()
        return(self.__actual_query_cur)
    
    @property
    def _updatecur(self):
        assert(self.dbcon != None)
        if( self.__actual_update_cur == None):
            self.__actual_update_cur = self.dbcon.cursor()
        return(self.__actual_update_cur)
        
    def _closedb(self):
        self.dbcon.close()
        
    def close_all(self):
        '''
        close all open files
        '''
        openfiles = list(self.open_files)
        for fileobj in openfiles:
            fileobj.close()
        
    def _create_tables(self):
        cur = self._updatecur
        cur.execute("CREATE TABLE IF NOT EXISTS FsFileMetaData(name text, fileid INTEGER, parent INTEGER)")
        cur.execute("CREATE TABLE IF NOT EXISTS FsDirMetaData(name text, fullpath TEXT, parentid INTEGER,\
                    created timestamp)")
        cur.execute("CREATE TABLE IF NOT EXISTS FsFileTable(type text, compression text, author TEXT, \
                    created timestamp, last_modified timestamp, last_accessed timestamp, \
                    locked BOOL, size INTEGER, contents BLOB)")
        
        #if the root directory name is created
        rootid = self._get_dir_id('/')
        if( rootid is None):
            cur.execute("INSERT INTO FsDirMetaData (name, fullpath) VALUES ('/','/')")
            
    def _get_dir_id(self, dirpath):
        '''
        get the id for given directory path.
        '''
        dirpath = remove_end_slash(dirpath)
        if( dirpath== None or len(dirpath)==0):
            dirpath = '/'
            
        self._querycur.execute("SELECT rowid from FsDirMetaData where fullpath=?",(dirpath,))
        dirid = None
        dirrow = fetchone(self._querycur)
        if( dirrow):
            dirid = dirrow[0]
        
        return(dirid)
        
    def _get_file_id(self, dir_id, filename):
        '''
        get the file id from the path
        '''
        assert(dir_id != None)
        assert(filename != None)
        file_id = None
        self._querycur.execute("select rowid from FsFileMetaData where name=? and parent=?",(filename,dir_id))
        row = fetchone(self._querycur)
        if( row ):
            file_id = row[0]
        return(file_id)
        
    def _get_file_contentid(self, file_id):
        '''
        return the file content id from the 'content' table (i.e. FsFileTable)        
        '''
        assert(file_id != None)
        content_id = None
        self._querycur.execute("select fileid from FsFileMetaData where ROWID=?",(file_id,))
        row = fetchone(self._querycur)
        assert(row != None)
        content_id = row[0]
        return(content_id)
        
    def _create_file_entry(self, dirid, filename, **kwargs):
        '''
        create file entry in the file table
        '''
        assert(dirid != None)
        assert(filename != None)
        #insert entry in file metadata table
        author = kwargs.pop('author', None)
        created = datetime.datetime.now().isoformat()
        last_modified = created
        compression = 'raw'
        size = 0
        self._updatecur.execute("INSERT INTO FsFileTable(author, compression, size, created, last_modified) \
                    values(?, ?, ?, ?, ?)",(author, compression, size, created, last_modified))
        content_id = self.dbcon.last_insert_rowid()
        #insert entry in file table
        self._updatecur.execute("INSERT INTO FsFileMetaData(name, parent, fileid) VALUES(?,?,?)",(filename, dirid, content_id))
        #self.dbcon.commit()
        fileid = self.dbcon.last_insert_rowid()
        return(fileid)
            
    def _writeblob(self, fileid, stream):
        '''
        extract the data from stream and write it as blob.
        '''
        size = stream.tell()
        last_modified = datetime.datetime.now().isoformat()
        self._updatecur.execute('UPDATE FsFileTable SET size=?, last_modified=?, contents=? where rowid=?',
                (size, last_modified, apsw.zeroblob(size), fileid))
        blob_stream=self.dbcon.blobopen("main", "FsFileTable", "contents", fileid, True) # 1 is for read/write
        stream.seek(0)
        blob_stream.write(stream.read())
        blob_stream.close()        
        
    def _on_close(self, fileobj):        
        #Unlock file on close.
        assert(fileobj != None and fileobj.id != None)
        self._lockfileentry(fileobj.id, lock=False)
        #Now remove it from openfile list.
        self.open_files.remove(fileobj)
            
    def _islocked(self, fileid):
        '''
        check if the file is locked.
        '''
        locked=False
        if( fileid):
            content_id = self._get_file_contentid(fileid)
            assert(content_id != None)
            self._querycur.execute("select locked from FsFileTable where rowid=?",(content_id,))
            row = fetchone(self._querycur)
            assert(row != None)
            locked = row[0]
        return(locked)
        
    def _lockfileentry(self, contentid, lock=True):
        '''
        lock the file entry in the database.
        '''
        assert(contentid != None)
        last_accessed=datetime.datetime.now().isoformat()
        self._updatecur.execute('UPDATE FsFileTable SET locked=?, last_accessed=? where rowid=?',
                    (lock, last_accessed, contentid))
        
    def _makedir(self, parent_id, dname):        
        self._querycur.execute("SELECT fullpath from FsDirMetaData where rowid=?",(parent_id,))
        row = fetchone(self._querycur)
        assert(row != None)
        parentpath = row[0]        
        fullpath= pathjoin(parentpath, dname)
        fullpath= remove_end_slash(fullpath)        
        created = datetime.datetime.now().isoformat()
        self._updatecur.execute('INSERT INTO FsDirMetaData(name, fullpath, parentid,created) \
                    VALUES(?,?,?,?)', (dname, fullpath, parent_id,created))
        
    def _rename_file(self, src, dst):
        '''
        rename source file 'src' to destination file 'dst'
        '''
        srcdir = dirname(src)
        srcfname = basename(src)
        dstdir = dirname(dst)
        dstfname = basename(dst)
        #Make sure that the destination directory exists and destination file
        #doesnot exist.
        dstdirid = self._get_dir_id(dstdir)
        if( dstdirid == None):
            raise ParentDirectoryMissingError(dst)
        dstfile_id = self._get_file_id(dstdirid, dstfname)
        if( dstfile_id != None):
            raise DestinationExistsError(dst)
        #All checks are done. Delete the entry for the source file.
        #Create an entry for the destination file.            
        
        srcdir_id = self._get_dir_id(srcdir)
        assert(srcdir_id != None)
        srcfile_id = self._get_file_id(srcdir_id, srcfname)
        assert(srcfile_id != None)
        srccontent_id = self._get_file_contentid(srcfile_id)
        self._updatecur.execute('DELETE FROM FsFileMetaData where ROWID=?',(srcfile_id,))
        self._updatecur.execute("INSERT INTO FsFileMetaData(name, parent, fileid) \
                            VALUES(?,?,?)",(dstfname, dstdirid, srccontent_id))
        
    def _rename_dir(self, src, dst):
        src = remove_end_slash(src)
        dst = remove_end_slash(dst)
        dstdirid = self._get_dir_id(dst)
        if( dstdirid != None):
            raise DestinationExistsError(dst)
        dstparent = dirname(dst)
        dstparentid = self._get_dir_id(dstparent)
        if(dstparentid == None):
            raise ParentDirectoryMissingError(dst)
        srcdirid = self._get_dir_id(src)
        assert(srcdirid != None)
        dstdname = basename(dst)        
        self._updatecur.execute('UPDATE FsDirMetaData SET name=?, fullpath=?, \
                    parentid=? where ROWID=?',(dstdname, dst, dstparentid, srcdirid,))
        
    def _get_dir_list(self, dirid, path, full):
        assert(dirid != None)
        assert(path != None)
        if( full==True):
            dirsearchpath = path + r'%'
            self._querycur.execute('SELECT fullpath FROM FsDirMetaData where fullpath LIKE ?',
                                   (dirsearchpath,))            
        else:
            #search inside this directory only
            self._querycur.execute('SELECT fullpath FROM FsDirMetaData where parentid=?',
                                   (dirid,))
        dirlist = [row[0] for row in self._querycur]        
        return dirlist
            
    def _get_file_list(self, dirpath, full):
        assert(dirpath != None)
                
        if( full==True):
            searchpath = dirpath + r"%"
            self._querycur.execute('SELECT FsFileMetaData.name, FsDirMetaData.fullpath \
                FROM FsFileMetaData, FsDirMetaData where FsFileMetaData.parent=FsDirMetaData.ROWID \
                    and FsFileMetaData.parent in (SELECT rowid FROM FsDirMetaData \
                    where fullpath LIKE ?)',(searchpath,))
        else:
            parentid = self._get_dir_id(dirpath)
            self._querycur.execute('SELECT FsFileMetaData.name, FsDirMetaData.fullpath \
                FROM FsFileMetaData, FsDirMetaData where FsFileMetaData.parent=FsDirMetaData.ROWID \
                    and FsFileMetaData.parent =?',(parentid,))
            
        filelist = [pathjoin(row[1],row[0]) for row in self._querycur]        
        return(filelist)
        
    def _get_dir_info(self, path):
        '''
        get the directory information dictionary.
        '''
        info = dict()
        info['st_mode'] = 0755
        return info
        
    def _get_file_info(self, path):
        filedir = dirname(path)
        filename = basename(path)
        dirid = self._get_dir_id(filedir)
        assert(dirid is not None)
        fileid = self._get_file_id(dirid, filename)
        assert(fileid is not None)
        contentid = self._get_file_contentid(fileid)
        assert(contentid is not None)
        self._querycur.execute('SELECT author, size, created, last_modified, last_accessed \
                        FROM FsFileTable where rowid=?',(contentid,))
        row = fetchone(self._querycur)
        assert(row != None)
        info = dict()
        info['author'] = row[0]
        info['size'] = row[1]
        info['created'] = row[2]
        info['last_modified'] = row[3]
        info['last_accessed'] = row[4]
        info['st_mode'] = 0666
        return(info)
        
    def _isfile(self,path):
        path = normpath(path)
        filedir = dirname(path)
        filename = basename(path)
        dirid = self._get_dir_id(filedir)        
        return(dirid is not None and self._get_file_id(dirid, filename) is not None)
        
    def _isdir(self,path):
        path = normpath(path)        
        return(self._get_dir_id(path) is not None)
        
    def _isexist(self,path):
        return self._isfile(path) or self._isdir(path)
        
    @synchronize
    def open(self, path, mode='r', **kwargs):
        self._initdb()
        path = normpath(path)
        filedir = dirname(path)
        filename = basename(path)
        
        dir_id = self._get_dir_id(filedir)
        if( dir_id == None):
            raise ResourceNotFoundError(filedir)
            
        file_id = self._get_file_id(dir_id, filename)        
        if( self._islocked(file_id)):
                raise ResourceLockedError(path)            
            
        sqfsfile=None
        if 'r' in mode:
            if file_id is None:
                raise ResourceNotFoundError(path)
            content_id = self._get_file_contentid(file_id)
            #make sure lock status is updated before the blob is opened
            self._lockfileentry(content_id, lock=True)
            blob_stream=self.dbcon.blobopen("main", "FsFileTable", "contents", file_id, False) # 1 is for read/write
            sqfsfile = SqliteReadableFile(self, path, content_id, blob_stream)            
                    
        elif 'w' in mode or 'a' in mode:
            if( file_id is None):
                file_id= self._create_file_entry(dir_id, filename)
                assert(file_id != None)
            
            content_id = self._get_file_contentid(file_id)
            #file_dir_entry.accessed_time = datetime.datetime.now()                        
            self._lockfileentry(content_id, lock=True)
            sqfsfile = SqliteWritableFile(self, path, content_id)            
            
        if( sqfsfile):
            self.open_files.append(sqfsfile)
            return sqfsfile
        
        raise ResourceNotFoundError(path)        
    
    @synchronize
    def isfile(self, path):
        self._initdb()
        return self._isfile(path)
        
    @synchronize
    def isdir(self, path):
        self._initdb()
        return self._isdir(path)
    
    @synchronize
    def listdir(self, path='/', wildcard=None, full=False, absolute=False, dirs_only=False, files_only=False):
        path = normpath(path)        
        dirid = self._get_dir_id(path)
        if( dirid == None):
            raise ResourceInvalidError(path)
        
        dirlist = self._get_dir_list(dirid, path,full)
        if( dirs_only):
            pathlist = dirlist
        else:            
            filelist = self._get_file_list(path, full)
            
            if( files_only == True):
                pathlist = filelist
            else:
                pathlist = filelist + dirlist
                
            
        if( wildcard and dirs_only == False):
            pass
        
        if( absolute == False):
            pathlist = map(lambda dpath:frombase(path,dpath), pathlist)
            
        return(pathlist)
        
    
    @synchronize
    def makedir(self, path, recursive=False, allow_recreate=False):
        self._initdb()
        path = remove_end_slash(normpath(path))
            
        if(self._isexist(path)==False):
            parentdir = dirname(path)
            dname = basename(path)
            
            parent_id = self._get_dir_id(parentdir)
            if( parent_id ==None):
                if( recursive == False):                
                    raise ParentDirectoryMissingError(path)
                else:
                    self.makedir(parentdir, recursive,allow_recreate)
                    parent_id = self._get_dir_id(parentdir)
            self._makedir(parent_id,dname)
        else:
            raise DestinationExistsError(path)
    
    @synchronize
    def remove(self, path):
        self._initdb()
        path = normpath(path)
        if( self.isdir(path)==True):
            #path is actually a directory
            raise ResourceInvalidError(path)
        
        filedir = dirname(path)
        filename = basename(path)
        dirid = self._get_dir_id(filedir)
        fileid = self._get_file_id(dirid, filename)
        if( fileid == None):
            raise ResourceNotFoundError(path)
        
        content_id = self._get_file_contentid(fileid)
        
        self._updatecur.execute("DELETE FROM FsFileMetaData where ROWID=?",(fileid,))
        #check there is any other file pointing to same location. If not
        #delete the content as well.
        self._querycur.execute('SELECT count(*) FROM FsFileMetaData where fileid=?',
                    (content_id,))
        row = fetchone(self._querycur)
        if( row == None or row[0] == 0):
            self._updatecur.execute("DELETE FROM FsFileTable where ROWID=?",(content_id,))            
    
    @synchronize
    def removedir(self,path, recursive=False, force=False):
        self._initdb()
        path = normpath(path)
        if( self.isfile(path)==True):
            #path is actually a file
            raise ResourceInvalidError(path)
        dirid = self._get_dir_id(path)
        if( dirid == None):
            raise ResourceNotFoundError(path)
        #check if there are any files in this directory
        self._querycur.execute("SELECT COUNT(*) FROM FsFileMetaData where parent=?",(dirid,))
        row = fetchone(self._qurycur)
        if( row[0] > 0):
            raise DirectoryNotEmptyError(path)
        self._updatecur.execute("DELETE FROM FsDirMetaData where ROWID=?",(dirid,))            
    
    @synchronize
    def rename(self,src, dst):
        self._initdb()
        src = normpath(src)
        dst = normpath(dst)
        if self._isexist(dst)== False:
            #first check if this is a directory rename or a file rename        
            if( self.isfile(src)):
                self._rename_file(src, dst)
            elif self.isdir(src):
                self._rename_dir(src, dst)
            else:
                raise ResourceNotFoundError(path)
        else:            
            raise DestinationExistsError(dst)
            
    @synchronize
    def getinfo(self, path):
        self._initdb()        
        path = normpath(path)
        isfile = False
        isdir = self.isdir(path)
        if( isdir == False):
            isfile=self.isfile(path)
        
        if( not isfile and not isdir):
            raise ResourceNotFoundError(path)
                    
        if isdir:
            info= self._get_dir_info(path)
        else:
            info= self._get_file_info(path)
        return(info)

#import msvcrt # built-in module
#
#def kbfunc():
#    return ord(msvcrt.getch()) if msvcrt.kbhit() else 0
#
#def mount_windows(sqlfilename, driveletter):
#    sqfs = SqliteFS(sqlfilename)
#    from fs.expose import dokan
#    #mp = dokan.mount(sqfs,driveletter,foreground=True)
#    #mp.unmount()
#    sqfs.close()
#    
#def run_tests(sqlfilename):
#    fs = SqliteFS(sqlfilename)
#    fs.makedir('/test')
#    f = fs.open('/test/test.txt', "w")
#    f.write("This is a test")
#    f.close()
#    f = fs.open('/test/test.txt', "r")
#    contents = f.read()
#    print contents
#    f.close()
#    print "testing file rename"
#    fs.rename('/test/test.txt', '/test/test1.txt')
#    f = fs.open('/test/test1.txt', "r")
#    print contents
#    f.close()
#    print "done testing file rename"
#    print "testing directory rename"
#    fs.rename('/test', '/test1')
#    f = fs.open('/test1/test1.txt', "r")
#    contents = f.read()
#    print contents
#    f.close()
#    print "done testing directory rename"
#    flist = fs.listdir('/', full=True,absolute=True,files_only=True)
#    print flist
#    fs.close()
#    
#if __name__ == '__main__':
#    run_tests("sqfs.sqlite")
#    mount_windows("sqfs.sqlite", 'm')
#    
#    #fs.remove('/test1/test1.txt')
#    #try:
#    #    f = fs.open('/test1/test1.txt', "r")
#    #except ResourceNotFoundError:
#    #    print "Success : file doesnot exist"
#    #fs.browse()
#    