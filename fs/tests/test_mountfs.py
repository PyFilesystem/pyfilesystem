from fs.mountfs import MountFS
from fs.memoryfs import MemoryFS
import unittest


class TestMountFS(unittest.TestCase):

    def test_auto_close(self):
        """Test MountFS auto close is working"""
        multi_fs = MountFS()
        m1 = MemoryFS()
        m2 = MemoryFS()
        multi_fs.mount('/m1', m1)
        multi_fs.mount('/m2', m2)
        self.assert_(not m1.closed)
        self.assert_(not m2.closed)
        multi_fs.close()
        self.assert_(m1.closed)
        self.assert_(m2.closed)

    def test_no_auto_close(self):
        """Test MountFS auto close can be disabled"""
        multi_fs = MountFS(auto_close=False)
        m1 = MemoryFS()
        m2 = MemoryFS()
        multi_fs.mount('/m1', m1)
        multi_fs.mount('/m2', m2)
        self.assert_(not m1.closed)
        self.assert_(not m2.closed)
        multi_fs.close()
        self.assert_(not m1.closed)
        self.assert_(not m2.closed)

    def test_mountfile(self):
        """Test mounting a file"""
        quote = b"""If you wish to make an apple pie from scratch, you must first invent the universe."""
        mem_fs = MemoryFS()
        mem_fs.makedir('foo')
        mem_fs.setcontents('foo/bar.txt', quote)
        foo_dir = mem_fs.opendir('foo')

        mount_fs = MountFS()
        mount_fs.mountfile('bar.txt', foo_dir.open, foo_dir.getinfo)

        self.assert_(mount_fs.isdir('/'))
        self.assert_(mount_fs.isdir('./'))
        self.assert_(mount_fs.isdir(''))

        # Check we can see the mounted file in the dir list
        self.assertEqual(mount_fs.listdir(), ["bar.txt"])
        self.assert_(not mount_fs.exists('nobodyhere.txt'))
        self.assert_(mount_fs.exists('bar.txt'))
        self.assert_(mount_fs.isfile('bar.txt'))
        self.assert_(not mount_fs.isdir('bar.txt'))

        # Check open and getinfo callables
        self.assertEqual(mount_fs.getcontents('bar.txt'), quote)
        self.assertEqual(mount_fs.getsize('bar.txt'), len(quote))

        # Check changes are written back
        mem_fs.setcontents('foo/bar.txt', 'baz')
        self.assertEqual(mount_fs.getcontents('bar.txt'), b'baz')
        self.assertEqual(mount_fs.getsize('bar.txt'), len('baz'))

        # Check changes are written to the original fs
        self.assertEqual(mem_fs.getcontents('foo/bar.txt'), b'baz')
        self.assertEqual(mem_fs.getsize('foo/bar.txt'), len('baz'))

        # Check unmount
        self.assert_(mount_fs.unmount("bar.txt"))
        self.assertEqual(mount_fs.listdir(), [])
        self.assert_(not mount_fs.exists('bar.txt'))

        # Check unount a second time is a null op, and returns False
        self.assertFalse(mount_fs.unmount("bar.txt"))

    def test_empty(self):
        """Test MountFS with nothing mounted."""
        mount_fs = MountFS()
        self.assertEqual(mount_fs.getinfo(''), {})
        self.assertEqual(mount_fs.getxattr('', 'yo'), None)
        self.assertEqual(mount_fs.listdir(), [])
        self.assertEqual(list(mount_fs.ilistdir()), [])
