from __future__ import absolute_import

from gobpersist import session
from accellion import gobphpapi

class CachingStorageEngine(session.StorageEngine):

    def __init__(self, storage_engine=None, cache=None):
        """storage engine backends"""
        self.storage_engine = storage_engine
        self.cache = cache(storage_engine=self.storage_engine)
        print 'DONE'

    def upload(self, gob, fp):
        """uploads a file to the storage engine"""

        gob2 = self.storage_engine.upload(gob, fp)

        return gob2

    def download(self, gob):
        """attempts to download file from cache, otherwise moves on to the storage engine"""
        gob2, iterable = self.cache.download(gob)

        return gob2, iterable

    def upload_iter(self, gob, iterable):
        """same as upload(), but includes a wrapper for an iterable"""
        gob2 = self.storage_engine.uplaod_iter(gob, iterable)
        
        return gob2
