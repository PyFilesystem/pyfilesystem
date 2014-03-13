"""
fs.appdirfs
===========

A collection of filesystems that map to application specific locations.

These classes abstract away the different requirements for user data across platforms,
which vary in their conventions. They are all subclasses of :class:`fs.osfs.OSFS`,
all that differs from `OSFS` is the constructor which detects the appropriate
location given the name of the application, author name and other parameters.

Uses `appdirs` (https://github.com/ActiveState/appdirs), written by Trent Mick and Sridhar Ratnakumar <trentm at gmail com; github at srid name>

"""

from fs.osfs import OSFS
from fs.appdirs import AppDirs

__all__ = ['UserDataFS',
           'SiteDataFS',
           'UserCacheFS',
           'UserLogFS']


class UserDataFS(OSFS):
    """A filesystem for per-user application data."""
    def __init__(self, appname, appauthor=None, version=None, roaming=False, create=True):
        """
        :param appname: the name of the application
        :param appauthor: the name of the author (used on Windows)
        :param version: optional version string, if a unique location per version of the application is required
        :param roaming: if True, use a *roaming* profile on Windows, see http://technet.microsoft.com/en-us/library/cc766489(WS.10).aspx
        :param create: if True (the default) the directory will be created if it does not exist

        """
        app_dirs = AppDirs(appname, appauthor, version, roaming)
        super(UserDataFS, self).__init__(app_dirs.user_data_dir, create=create)


class SiteDataFS(OSFS):
    """A filesystem for application site data."""
    def __init__(self, appname, appauthor=None, version=None, roaming=False, create=True):
        """
        :param appname: the name of the application
        :param appauthor: the name of the author (not used on linux)
        :param version: optional version string, if a unique location per version of the application is required
        :param roaming: if True, use a *roaming* profile on Windows, see http://technet.microsoft.com/en-us/library/cc766489(WS.10).aspx
        :param create: if True (the default) the directory will be created if it does not exist

        """
        app_dirs = AppDirs(appname, appauthor, version, roaming)
        super(SiteDataFS, self).__init__(app_dirs.site_data_dir, create=create)


class UserCacheFS(OSFS):
    """A filesystem for per-user application cache data."""
    def __init__(self, appname, appauthor=None, version=None, roaming=False, create=True):
        """
        :param appname: the name of the application
        :param appauthor: the name of the author (not used on linux)
        :param version: optional version string, if a unique location per version of the application is required
        :param roaming: if True, use a *roaming* profile on Windows, see http://technet.microsoft.com/en-us/library/cc766489(WS.10).aspx
        :param create: if True (the default) the directory will be created if it does not exist

        """
        app_dirs = AppDirs(appname, appauthor, version, roaming)
        super(UserCacheFS, self).__init__(app_dirs.user_cache_dir, create=create)


class UserLogFS(OSFS):
    """A filesystem for per-user application log data."""
    def __init__(self, appname, appauthor=None, version=None, roaming=False, create=True):
        """
        :param appname: the name of the application
        :param appauthor: the name of the author (not used on linux)
        :param version: optional version string, if a unique location per version of the application is required
        :param roaming: if True, use a *roaming* profile on Windows, see http://technet.microsoft.com/en-us/library/cc766489(WS.10).aspx
        :param create: if True (the default) the directory will be created if it does not exist

        """
        app_dirs = AppDirs(appname, appauthor, version, roaming)
        super(UserLogFS, self).__init__(app_dirs.user_log_dir, create=create)


if __name__ == "__main__":
    udfs = UserDataFS('exampleapp', appauthor='pyfs')
    print udfs
    udfs2 = UserDataFS('exampleapp2', appauthor='pyfs', create=False)
    print udfs2
