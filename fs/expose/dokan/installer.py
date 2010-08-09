
import os
import shutil
import subprocess
import ctypes

kernel32 = ctypes.windll.kernel32

def GetSystemDirectory():
    buf = ctypes.create_unicode_buffer(260)
    if not kernel32.GetSystemDirectoryW(ctypes.byref(buf),260):
        raise ctypes.WinError()
    return buf.value


def _tag2vertup(tag):
    bits = []
    for bit in tag.split("-"):
        for bit2 in bit.split("."):
            try:
                bits.append(int(bit2.strip()))
            except ValueError:
                pass
    return tuple(bits)

def install_dokan(release_dir,vendorid="pyfilesystem"):
    """Install dokan from the given release directory."""
    reltag = os.path.basename(release_dir)
    newver = _tag2vertup(reltag)
    sysdir = GetSystemDirectory()
    pfdir = os.path.join(os.environ["PROGRAMFILES"],"Dokan")
    #  Is Dokan already installed, and is it safe to upgrade?
    old_reltag = None
    if os.path.exists(os.path.join(sysdir,"drivers","dokan.sys")):
        for nm in os.listdir(pfdir):
            if nm.startswith(vendorid):
                old_reltag = nm[len(vendorid)+1:-4]
                oldver = _tag2vertup(old_reltag)
                if oldver >= newver:
                    raise OSError("dokan already at version " + reltag)
                break
        else:
            raise OSError("dokan already installed from another source")
    #  Device what version to install based on windows version.
    wver = sys.getwindowsversion()
    if wver < (5,1):
        raise OSError("windows is too old to install dokan")
    if wver < (6,0):
        wtyp = "wxp"
    elif wver < (6,1):
        wtyp = "wlh"
    else:
        wtyp = "win7"
    srcdir = os.path.join(release_dir,wtyp)
    # Terminate the existing install and remove it
    if old_reltag:
        uninstall_dokan(old_reltag)
    # Copy new files to the appropriate place
    if not os.path.exists(pfdir):
        os.makedirs(pfdir)
    f = open(os.path.join(pfdir,vendorid+"-"+reltag+".txt"),"wt")
    try:
        f.write("Dokan automatically installed by " + vendorid + "\n")
    finally:
        f.close()
    shutil.copy2(os.path.join(srcdir,"dll","dokan.dll"),
                 os.path.join(sysdir,"dokan.dll"))
    shutil.copy2(os.path.join(srcdir,"sys","dokan.sys"),
                 os.path.join(sysdir,"drivers","dokan.sys"))
    shutil.copy2(os.path.join(srcdir,"mounter","mounter.exe"),
                 os.path.join(pfdir,"mounter.exe"))
    shutil.copy2(os.path.join(srcdir,"dokanctrl","dokanctl.exe"),
                 os.path.join(pfdir,"dokanctl.exe"))
    #  Invoke dokanctl to install the drivers
    _dokanctl(pfdir,"/i","a")
    

def uninstall_dokan(release_dir):
    reltag = os.path.basename(release_dir)
    newver = _tag2vertup(reltag)
    sysdir = GetSystemDirectory()
    pfdir = os.path.join(os.environ["PROGRAMFILES"],"Dokan")
    if dokan_installed:
        _dokanctl(pfdir,"/r","a")
        os.unlink(os.path.join(sysdir,"drivers","dokan.sys"))
        os.unlink(os.path.join(sysdir,"dokan.dll"))
        for nm in os.listdir(pfdir):
            os.unlink(os.path.join(pfdir,nm))

def _dokanctl(pfdir,*args):
    dokanctl = os.path.join(pfdir,"dokanctl.exe")
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    p = subprocess.Popen([dokanctl]+list(args),startupinfo=startupinfo,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    if p.wait() != 0:
        raise OSError("dokanctl failed: " + p.stdout.read())


if __name__ == "__main__":
    import sys
    install_dokan(sys.argv[1])

