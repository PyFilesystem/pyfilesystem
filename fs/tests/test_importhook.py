
import sys
import unittest
import marshal
import imp
import struct
from textwrap import dedent

from fs.expose.importhook import FSImportHook
from fs.tempfs import TempFS
from fs.zipfs import ZipFS

from six import b


class TestFSImportHook(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        for mph in list(sys.meta_path):
            if isinstance(mph,FSImportHook):
                sys.meta_path.remove(mph)
        for ph in list(sys.path_hooks):
            if issubclass(ph,FSImportHook):
                sys.path_hooks.remove(mph)
        for (k,v) in sys.modules.items():
            if k.startswith("fsih_"):
                del sys.modules[k]
            elif hasattr(v,"__loader__"):
                if isinstance(v.__loader__,FSImportHook):
                    del sys.modules[k]
        sys.path_importer_cache.clear()

    def _init_modules(self,fs):
        fs.setcontents("fsih_hello.py",b(dedent("""
            message = 'hello world!'
        """)))
        fs.makedir("fsih_pkg")
        fs.setcontents("fsih_pkg/__init__.py",b(dedent("""
            a = 42
        """)))
        fs.setcontents("fsih_pkg/sub1.py",b(dedent("""
            import fsih_pkg
            from fsih_hello import message
            a = fsih_pkg.a
        """)))
        fs.setcontents("fsih_pkg/sub2.pyc",self._getpyc(b(dedent("""
            import fsih_pkg
            from fsih_hello import message
            a = fsih_pkg.a * 2
        """))))

    def _getpyc(self,src):
        """Get the .pyc contents to match th given .py source code."""
        code = imp.get_magic() + struct.pack("<i",0)
        code += marshal.dumps(compile(src,__file__,"exec"))
        return code

    def test_loader_methods(self):
        t = TempFS()
        self._init_modules(t)
        ih = FSImportHook(t)
        sys.meta_path.append(ih)
        try:
            self.assertEquals(ih.find_module("fsih_hello"),ih)
            self.assertEquals(ih.find_module("fsih_helo"),None)
            self.assertEquals(ih.find_module("fsih_pkg"),ih)
            self.assertEquals(ih.find_module("fsih_pkg.sub1"),ih)
            self.assertEquals(ih.find_module("fsih_pkg.sub2"),ih)
            self.assertEquals(ih.find_module("fsih_pkg.sub3"),None)
            m = ih.load_module("fsih_hello")
            self.assertEquals(m.message,"hello world!")
            self.assertRaises(ImportError,ih.load_module,"fsih_helo")
            ih.load_module("fsih_pkg")
            m = ih.load_module("fsih_pkg.sub1")
            self.assertEquals(m.message,"hello world!")
            self.assertEquals(m.a,42)
            m = ih.load_module("fsih_pkg.sub2")
            self.assertEquals(m.message,"hello world!")
            self.assertEquals(m.a,42 * 2)
            self.assertRaises(ImportError,ih.load_module,"fsih_pkg.sub3")
        finally:
            sys.meta_path.remove(ih)
            t.close()

    def _check_imports_are_working(self):
        try:
            import fsih_hello
            self.assertEquals(fsih_hello.message,"hello world!")
            try:
                import fsih_helo
            except ImportError:
                pass
            else:
                assert False, "ImportError not raised"
            import fsih_pkg
            import fsih_pkg.sub1
            self.assertEquals(fsih_pkg.sub1.message,"hello world!")
            self.assertEquals(fsih_pkg.sub1.a,42)
            import fsih_pkg.sub2
            self.assertEquals(fsih_pkg.sub2.message,"hello world!")
            self.assertEquals(fsih_pkg.sub2.a,42 * 2)
            try:
                import fsih_pkg.sub3
            except ImportError:
                pass
            else:
                assert False, "ImportError not raised"
        finally:
            for k in sys.modules.keys():
                if k.startswith("fsih_"):
                    del sys.modules[k]

    def test_importer_on_meta_path(self):
        t = TempFS()
        self._init_modules(t)
        ih = FSImportHook(t)
        sys.meta_path.append(ih)
        try:
            self._check_imports_are_working()
        finally:
            sys.meta_path.remove(ih)
            t.close()

    def test_url_on_sys_path(self):
        t = TempFS()
        zpath = t.getsyspath("modules.zip")
        z = ZipFS(zpath,"w")
        self._init_modules(z)
        z.close()
        z = ZipFS(zpath,"r")
        assert z.isfile("fsih_hello.py")
        z.close()
        sys.path.append("zip://" + zpath)
        FSImportHook.install()
        try:
            self._check_imports_are_working()
        finally:
            sys.path_hooks.remove(FSImportHook)
            sys.path.pop()
            t.close()
