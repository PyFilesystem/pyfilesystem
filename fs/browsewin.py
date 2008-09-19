#!/usr/bin/env python
"""
Creates a window which can be used to browse the contents of a filesystem.
To use, call the 'browse' method with a filesystem object. Double click a file
or directory to display its properties.

Requires wxPython.

"""

import wx
import wx.gizmos

import fs

class InfoFrame(wx.Frame):

    def __init__(self, path, desc, info):
        wx.Frame.__init__(self, None, -1, style=wx.DEFAULT_FRAME_STYLE, size=(500, 500))

        self.SetTitle("FS Object info - %s (%s)" % (path, desc))

        keys = info.keys()
        keys.sort()

        self.list_ctrl = wx.ListCtrl(self, -1, style=wx.LC_REPORT|wx.SUNKEN_BORDER)

        self.list_ctrl.InsertColumn(0, "Key")
        self.list_ctrl.InsertColumn(1, "Value")

        self.list_ctrl.SetColumnWidth(0, 190)
        self.list_ctrl.SetColumnWidth(1, 300)

        for key in keys:
            self.list_ctrl.Append((key, repr(info[key])))



class BrowseFrame(wx.Frame):

    def __init__(self, fs):

        wx.Frame.__init__(self, None, size=(1000, 600))

        self.fs = fs
        self.SetTitle("FS Browser - "+str(fs))

        self.tree = wx.gizmos.TreeListCtrl(self, -1, style=wx.TR_DEFAULT_STYLE | wx.TR_HIDE_ROOT)
        self.tree.AddColumn("File System", 300)
        self.tree.AddColumn("Description", 250)
        self.tree.AddColumn("Size", 150)
        self.tree.AddColumn("Created", 250)
        self.root_id = self.tree.AddRoot('root', data=wx.TreeItemData( {'path':"/", 'expanded':False} ))

        rid = self.tree.GetItemData(self.root_id)

        isz = (16, 16)
        il = wx.ImageList(isz[0], isz[1])
        self.fldridx     = il.Add(wx.ArtProvider_GetBitmap(wx.ART_FOLDER,      wx.ART_OTHER, isz))
        self.fldropenidx = il.Add(wx.ArtProvider_GetBitmap(wx.ART_FILE_OPEN,   wx.ART_OTHER, isz))
        self.fileidx     = il.Add(wx.ArtProvider_GetBitmap(wx.ART_NORMAL_FILE, wx.ART_OTHER, isz))

        self.tree.SetImageList(il)
        self.il = il

        self.tree.SetItemImage(self.root_id, self.fldridx, wx.TreeItemIcon_Normal)
        self.tree.SetItemImage(self.root_id, self.fldropenidx, wx.TreeItemIcon_Expanded)

        self.Bind(wx.EVT_TREE_ITEM_EXPANDING, self.OnItemExpanding)
        self.Bind(wx.EVT_TREE_ITEM_ACTIVATED, self.OnItemActivated)


        wx.CallAfter(self.OnInit)

    def OnInit(self):

        self.expand(self.root_id)


    def expand(self, item_id):

        item_data = self.tree.GetItemData(item_id).GetData()

        path = item_data["path"]

        if not self.fs.isdir(path):
            return

        if item_data['expanded']:
            return

        paths = [(self.fs.isdir(p), p) for p in self.fs.listdir(path, absolute=True)]

        if not paths:
            self.tree.SetItemHasChildren(item_id, False)
            self.tree.Collapse(item_id)
            return

        paths.sort(key=lambda p:(not p[0], p[1].lower()))

        for is_dir, new_path in paths:

            name = fs.pathsplit(new_path)[-1]

            new_item = self.tree.AppendItem(item_id, name, data=wx.TreeItemData({'path':new_path, 'expanded':False}))

            info = self.fs.getinfo(new_path)

            if is_dir:

                self.tree.SetItemHasChildren(new_item)
                self.tree.SetItemImage(new_item, self.fldridx, 0, wx.TreeItemIcon_Normal)
                self.tree.SetItemImage(new_item, self.fldropenidx, 0, wx.TreeItemIcon_Expanded)

                self.tree.SetItemText(new_item, "", 2)

                ct = info.get('created_time', None)
                if ct is not None:
                    self.tree.SetItemText(new_item, ct.ctime(), 3)
                else:
                    self.tree.SetItemText(new_item, 'unknown', 3)

            else:
                self.tree.SetItemImage(new_item, self.fileidx, 0, wx.TreeItemIcon_Normal)

                self.tree.SetItemText(new_item, str(info.get('size', '?'))+ " bytes", 2)

                ct = info.get('created_time', None)
                if ct is not None:
                    self.tree.SetItemText(new_item, ct.ctime(), 3)
                else:
                    self.tree.SetItemText(new_item, 'unknown', 3)

            self.tree.SetItemText(new_item, self.fs.desc(new_path), 1)

        item_data['expanded'] = True
        self.tree.Expand(item_id)


    def OnItemExpanding(self, e):

        self.expand(e.GetItem())
        e.Skip()

    def OnItemActivated(self, e):

        item_data = self.tree.GetItemData(e.GetItem()).GetData()
        path = item_data["path"]
        info = self.fs.getinfo(path)

        info_frame = InfoFrame(path, self.fs.desc(path), info)
        info_frame.Show()

def browse(fs):

    app = wx.PySimpleApp()
    frame = BrowseFrame(fs)
    frame.Show()
    app.MainLoop()

if __name__ == "__main__":

    from osfs import OSFS
    home_fs = OSFS("~/")
    browse(home_fs)
