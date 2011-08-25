import hashlib
import os

class SizeFile(object):
    """A file-like object which will monitor the size that's been read
    and set storable.size accordingly."""

    def __init__(self, storable, fp):
        self.fp = fp
        self.storable = storable
    def read(self, size):
        ret = self.fp.read(size)
        self.storable.size += len(ret)
        return ret


class LimitedFile(object):
    """A file-like object which will ensure only a certain amount of
    data can be read."""

    def __init__(self, storable, fp):
        self.fp = fp
        self.remaining = storable.size.value
    def read(self, size):
        print "Remaining: %i\nRequested: %i" % (self.remaining, size)
        if not self.remaining:
            return ""
        if size >= self.remaining:
            ret = self.fp.read(self.remaining)
        else:
            ret = self.fp.read(size)
        print "Read: %i" % (len(ret),)
        self.remaining -= len(ret)
        return ret


class Storable(object):
    """Addon subclass for Gobs to give them a data fork, sorta like in
    the HFS filesystem.

    Assumes that the object has the fields 'size' and 'mime_type' defined
    somehow.
    """

    def __init__(self, *args, **kwargs):
        super(Storable, self).__init__()

    def upload(self, fp):
        """Upload data to this object."""
        if not getattr(self, 'size', False):
            try:
                self.size = os.fstat(fp)[7]
            except:
                self.size = 0
        if self.size:
            fp = LimitedFile(self, fp)
        else:
            fp = SizeFile(self, fp)
        self.session.upload(self, fp)

    def upload_iter(self, upload_iter):
        """Upload data to this object, iterable version."""
        if not getattr(self, 'size', False):
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
        self.fp = fp
        self.md5 = md5
    def read(self, size):
        ret = self.fp.read(size)
        self.md5.update(ret)
        return ret


class MD5Storable(Storable):
    """Superclass of storable that will set a field named 'md5sum' to
    the md5 sum of the uploaded data."""

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
