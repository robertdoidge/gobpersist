from __future__ import absolute_import

from gobpersist import session
from accellion import gobphpapi
from wsgiref.util import FileWrapper

class FileStorageBackend(session.StorageEngine):

    def __init__(self, storage_engine=None, cache=None):
        """storage engine backends"""
        self.storage_engine = storage_engine
        self.cache = cache
        
    def upload(self, gob, fp):
        """uploads a file to the storage engine"""

        gob2 = self.storage_engine.upload(gob, fp)

        return gob2

    def download(self, gob):
        """attempts to download file from cache, otherwise moves on to the storage engine"""
        (gob2, iterable) = self.cache.download(gob)
        if iterable == -1:
            (gob2, iterable) = self.storage_engine.download(gob)
            self.cache.upload(gob, iterable)
            (gob2, iterable) = self.cache.download(gob)
        if iterable == -1:
            (gob2, iterable) = self.storage_engine.download(gob)
        return (gob2, iterable)

    def upload_iter(self, gob, iterable):
        """same as upload(), but includes a wrapper for an iterable"""

        gob2 = self.storage_engine.upload_iter(gob, iterable)
        
        return gob2
