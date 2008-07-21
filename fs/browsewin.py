#!/usr/bin/env python

import wx

import fs



class BrowseFrame(wx.Frame):

    def __init__(self, fs):
        
        wx.Frame.__init__(self, None)

        self.fs = fs
        self.SetTitle("FS Browser - "+str(fs))

        self.tree = wx.TreeCtrl(self, -1)
        self.root_id = self.tree.AddRoot(str(fs), data=wx.TreeItemData( {'path':"/", 'expanded':False} ))

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

            if is_dir:
                self.tree.SetItemHasChildren(new_item)
                self.tree.SetItemImage(new_item, self.fldridx, wx.TreeItemIcon_Normal)
                self.tree.SetItemImage(new_item, self.fldropenidx, wx.TreeItemIcon_Expanded)
            else:
                self.tree.SetItemImage(new_item, self.fileidx, wx.TreeItemIcon_Normal)

        item_data['expanded'] = True
        self.tree.Expand(item_id)


    def OnItemExpanding(self, e):

        self.expand(e.GetItem())
        e.Skip()


def browse(fs):

    app = wx.PySimpleApp()
    frame = BrowseFrame(fs)
    frame.Show()
    app.MainLoop()

if __name__ == "__main__":

    home_fs = fs.OSFS("~/")
    browse(home_fs)
