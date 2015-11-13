#  Copyright (c) 2009-2010, Cloud Matrix Pty. Ltd.
#  All rights reserved; available under the terms of the MIT License.
"""

  fs.contrib.davfs.xmlobj:  dexml model definitions for WebDAV

This module defines the various XML element structures for WebDAV as a set
of dexml.Model subclasses.

"""

from urlparse import urlparse, urlunparse

from httplib import responses as STATUS_CODE_TEXT
STATUS_CODE_TEXT[207] = "Multi-Status"

import dexml
from dexml import fields

Error = dexml.Error


class _davbase(dexml.Model):
    """Base class for all davfs XML models."""

    class meta:
        namespace = "DAV:"
        namespace_prefix = "D"
        order_sensitive = False


class HrefField(fields.String):
    """Field representing a <href> tag."""

    def __init__(self,*args,**kwds):
        kwds["tagname"] = "href"
        super(HrefField,self).__init__(*args,**kwds)

    def parse_value(self,value):
        url = urlparse(value.encode("UTF-8"))
        return urlunparse((url.scheme,url.netloc,url.path,url.params,url.query,url.fragment))

    def render_value(self,value):
        url = urlparse(value.encode("UTF-8"))
        return urlunparse((url.scheme,url.netloc,url.path,url.params,url.query,url.fragment))


class TimeoutField(fields.Field):
    """Field representing a WebDAV timeout value."""

    def __init__(self,*args,**kwds):
        if "tagname" not in kwds:
            kwds["tagname"] = "timeout"
        super(TimeoutField,self).__init__(*args,**kwds)

    @classmethod
    def parse_value(cls,value):
        if value == "Infinite":
            return None
        if value.startswith("Second-"):
            return int(value[len("Second-"):])
        raise ValueError("invalid timeout specifier: %s" % (value,))

    def render_value(self,value):
        if value is None:
            return "Infinite"
        else:
            return "Second-" + str(value)


class StatusField(fields.Value):
    """Field representing a WebDAV status-line value.

    The value may be set as either a string or an integer, and is converted
    into a StatusString instance.
    """

    def __init__(self,*args,**kwds):
        kwds["tagname"] = "status"
        super(StatusField,self).__init__(*args,**kwds)

    def __get__(self,instance,owner):
        val = super(StatusField,self).__get__(instance,owner)
        if val is not None:
            val = StatusString(val,instance,self)
        return val

    def __set__(self,instance,value):
        if isinstance(value,basestring):
            # sanity check it
            bits = value.split(" ")
            if len(bits) < 3 or bits[0] != "HTTP/1.1":
                raise ValueError("Not a valid status: '%s'" % (value,))
            int(bits[1])
        elif isinstance(value,int):
            # convert it to a message
            value = StatusString._value_for_code(value)
        super(StatusField,self).__set__(instance,value)


class StatusString(str):
    """Special string representing a HTTP status line.

    It's a string, but it exposes the integer attribute "code" giving just
    the actual response code.
    """

    def __new__(cls,val,inst,owner):
        return str.__new__(cls,val)

    def __init__(self,val,inst,owner):
         self._owner = owner
         self._inst = inst

    @staticmethod
    def _value_for_code(code):
        msg = STATUS_CODE_TEXT.get(code,"UNKNOWN STATUS CODE")
        return "HTTP/1.1 %d %s" % (code,msg)

    def _get_code(self):
        return int(self.split(" ")[1])
    def _set_code(self,code):
        newval = self._value_for_code(code)
        self._owner.__set__(self._inst,newval)
    code = property(_get_code,_set_code)


class multistatus(_davbase):
    """XML model for a multi-status response message."""
    responses = fields.List("response",minlength=1)
    description = fields.String(tagname="responsedescription",required=False)


class response(_davbase):
    """XML model for an individual response in a multi-status message."""
    href = HrefField()
    # TODO: ensure only one of hrefs/propstats
    hrefs = fields.List(HrefField(),required=False)
    status = StatusField(required=False)
    propstats = fields.List("propstat",required=False)
    description = fields.String(tagname="responsedescription",required=False)


class propstat(_davbase):
    """XML model for a propstat response message."""
    props = fields.XmlNode(tagname="prop",encoding="UTF-8")
    status = StatusField()
    description = fields.String(tagname="responsedescription",required=False)


class propfind(_davbase):
    """XML model for a propfind request message."""
    allprop = fields.Boolean(tagname="allprop",required=False)
    propname = fields.Boolean(tagname="propname",required=False)
    prop = fields.XmlNode(tagname="prop",required=False,encoding="UTF-8")


class propertyupdate(_davbase):
    """XML model for a propertyupdate request message."""
    commands = fields.List(fields.Choice("remove","set"))

class remove(_davbase):
    """XML model for a propertyupdate remove command."""
    props = fields.XmlNode(tagname="prop",encoding="UTF-8")

class set(_davbase):
    """XML model for a propertyupdate set command."""
    props = fields.XmlNode(tagname="prop",encoding="UTF-8")

class lockdiscovery(_davbase):
    """XML model for a lockdiscovery request message."""
    locks = fields.List("activelock")

class activelock(_davbase):
    """XML model for an activelock response message."""
    lockscope = fields.Model("lockscope")
    locktype = fields.Model("locktype")
    depth = fields.String(tagname="depth")
    owner = fields.XmlNode(tagname="owner",encoding="UTF-8",required=False)
    timeout = TimeoutField(required=False)
    locktoken = fields.Model("locktoken",required=False)

class lockscope(_davbase):
    """XML model for a lockscope response message."""
    shared = fields.Boolean(tagname="shared",empty_only=True)
    exclusive = fields.Boolean(tagname="exclusive",empty_only=True)

class locktoken(_davbase):
    """XML model for a locktoken response message."""
    tokens = fields.List(HrefField())

class lockentry(_davbase):
    """XML model for a lockentry response message."""
    lockscope = fields.Model("lockscope")
    locktype = fields.Model("locktype")

class lockinfo(_davbase):
    """XML model for a lockinfo response message."""
    lockscope = fields.Model("lockscope")
    locktype = fields.Model("locktype")
    owner = fields.XmlNode(tagname="owner",encoding="UTF-8")

class locktype(_davbase):
    """XML model for a locktype response message."""
    type = fields.XmlNode(encoding="UTF-8")


