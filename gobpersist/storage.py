import hashlib
import os

class SizeFile(object):
    def __init__(self, storable, fp):
        self.fp = fp
        self.storable = storable
    def read(self, size):
        ret = self.fp.read(size)
        self.storable.size += len(ret)
        return ret

class LimitedFile(object):
    def __init__(self, storable, fp):
        self.fp = fp
        self.remaining = storable.size
    def read(self, size):
        if not self.remaining:
            return ""
        if size >= self.remaining:
            ret = self.fp.read(self.remaining)
        else:
            ret = self.fp.read(size)
        self.remaining -= len(ret)
        return ret

class Storable(object):
    def __init__(self, *args, **kwargs):
        super(Storable, self).__init__()

    def upload(self, fp):
        if not getattr(self, 'size', False):
            try:
                self.size = os.fstat(fp)[7]
            except:
                self.size = 0
        if self.size:
            fp = LimitedFile(self, fp)
        else:
            fp = SizeFile(self, fp)
        return self.session.upload(self, fp)

    def upload_iter(self, upload_iter):
        if not getattr(self, 'size', False):
            self.size = 0
            def wrapped_upload_iter():
                for r in upload_iter:
                    self.size += len(r)
                    yield r
            upload_iter = wrapped_upload_iter()
        return self.session.upload_iter(self, upload_iter)

    def download(self):
        return self.session.download(self)

class MD5File(object):
    def __init__(self, md5, fp):
        self.fp = fp
        self.md5 = md5
    def read(self, size):
        ret = self.fp.read(size)
        self.md5.update(ret)
        return ret

class MD5Storable(Storable):
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
