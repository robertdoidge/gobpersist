# storage.py - Abstract classes for use in storing large amounts of
# binary data.
# Copyright (C) 2012 Accellion, Inc.
#
# This library is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation; version 2.1.
#
# This library is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301 USA
"""The ``Storage`` class and utility classes for common needs of a
:class:`gobpersist.session.StorageEngine`.

.. moduleauthor:: Evan Buswell <evan.buswell@accellion.com>
"""

import hashlib
import os

class SizeFile(object):
    """A file-like object which will monitor the size that's been read
    and set storable.size accordingly."""

    def __init__(self, storable, fp):
        """
        Args:
           ``storable``: The storable which this file represents.

           ``fp``: The :class:`file` for this storable.
        """
        self.fp = fp
        self.storable = storable

    def read(self, size):
        ret = self.fp.read(size)
        self.storable.size += len(ret)
        return ret

    def __getattr__(self, name):
        return getattr(self.fp, name)


class LimitedFile(object):
    """A file-like object which will ensure only a certain amount of
    data can be read."""

    def __init__(self, storable, fp):
        """
        Args:
           ``storable``: The storable which this file represents.

           ``fp``: The :class:`file` for this storable.
        """
        self.fp = fp
        self.remaining = storable.size.value

    def read(self, size):
        if not self.remaining:
            return ""
        if size >= self.remaining:
            ret = self.fp.read(self.remaining)
        else:
            ret = self.fp.read(size)
        self.remaining -= len(ret)
        return ret

    def __getattr__(self, name):
        return getattr(self.fp, name)


class Storable(object):
    """Addon subclass for a :class:`gobpersist.gob.Gob` to give it a
    data fork, sorta like in the HFS filesystem.

    Assumes that the object has the fields ``size`` and ``mime_type``
    defined somehow.
    """

    def __init__(self, *args, **kwargs):
        super(Storable, self).__init__()

    def upload(self, fp):
        """Upload data to this object."""
        if getattr(self, 'size', None) is None:
            try:
                self.size = os.fstat(fp)[6]
            except:
                self.size = None
        if self.size is not None:
            fp = LimitedFile(self, fp)
        else:
            fp = SizeFile(self, fp)
        self.session.upload(self, fp)

    def upload_iter(self, upload_iter):
        """Upload data to this object, iterable version."""
        if getattr(self, 'size', None) is None:
            self.size = 0
            def wrapped_upload_iter():
                for r in upload_iter:
                    self.size += len(r)
                    yield r
            upload_iter = wrapped_upload_iter()
        self.session.upload_iter(self, upload_iter)

    def download(self):
        """Return an iterable yielding the data for this object."""
        return self.session.download(self)


class MD5File(object):
    """File-like object that monitors read contents and yields an MD5
    sum."""

    def __init__(self, md5, fp):
        """
        Args:
           ``md5``: An instance of :class:`hashlib.md5`.

           ``fp``: The :class:`file` for this storable.
        """
        self.fp = fp
        self.md5 = md5

    def read(self, size):
        ret = self.fp.read(size)
        self.md5.update(ret)
        return ret

    def __getattr__(self, name):
        return getattr(self.fp, name)


class MD5Storable(Storable):
    """Subclass of :class:`Storable` that will
    automatically set a field named ``md5sum`` to the md5 sum of the
    uploaded data."""

    def __init__(self, *args, **kwargs):
        super(MD5Storable, self).__init__(*args, **kwargs)

    def upload(self, fp):
        md5 = hashlib.md5()
        fp = MD5File(md5, fp)
        ret = super(MD5Storable, self).upload(fp)
        self.md5sum = md5.hexdigest()
        return ret

    def upload_iter(self, upload_iter):
        md5 = hashlib.md5()
        def wrapped_upload_iter():
            for r in upload_iter:
                md5.update(r)
                yield r
        ret = super(MD5Storable, self).upload_iter(wrapped_upload_iter())
        self.md5sum = md5.hexdigest()
        return ret
