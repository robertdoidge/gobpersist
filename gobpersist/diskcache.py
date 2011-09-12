from __future__ import absolute_import

from gobpersist import minifs
from gobpersist import session

import os,sys
import pyrant
import uuid

import jsonpickle

# diskcache.py
# utilizes pyrant, tokyotyrant, tokyocabinet, and minifs.py to
# implement backend storage and caching for user files

KEYFIELD = 'file_handle'
"""the File field we want to use to generate keys, accellion/schema.py"""

class TokyoDiskStore(session.StorageEngine):
    """TokyoTyrant client and TokyoCabinet dbm for storage control"""
    def __init__(self, server='127.0.0.1', port=1978, permstore=None, cachepath='accellion_cache', cachesize=2000):

        self.perm_storage = permstore
        """permanent storage backend to be used with this cache"""

        self.tt = pyrant.Tyrant(server, port)

        """tokyo cabinet client"""
        self.filerecord_cache = []
        self.sizerecord_cache = {}
        self.cache_spaceleft = 0

        try:
            self.filerecord_cache = jsonpickle.decode(self.tt['filerec_cache'])
            self.sizerecord_cache = jsonpickle.decode(self.tt['sizerec_cache'])
            self.cache_spaceleft = jsonpickle.decode(self.tt['cachespace'])
        except KeyError:
            print 'this is the first time setting up this storage engine.'
            self.clean_directory()
            self.filerecord_cache = -1
                
        self.filecache_directory = cachepath
        """directories for storage"""

        self.filecache_size = cachesize
        """sizes of storage containers"""

        self.cache_storage = minifs.MRUGobPreserve(self.filecache_directory, self.filecache_size)
        """temporary disk storage"""        
        
        if self.filerecord_cache != -1:
            self.cache_storage.recordregistry = self.filerecord_cache
            self.cache_storage.sizeregistry = self.sizerecord_cache
            self.cache_storage.partremsize = self.cache_spaceleft

    def clean_directory(self):
        for elem in os.listdir(cachepath):
            path = os.path.join(cachepath, elem)
            try:
                os.unlink(file_path)
            except Exception, e:
                print e

    def store_permanent_records(self):
        """in case of a system halt, recall saved file information"""
        self.tt['filerec_cache'] = jsonpickle.encode(self.cache_storage.recordregistry)
        self.tt['sizerec_cache'] = jsonpickle.encode(self.cache_storage.sizeregistry)
        self.tt['cachespace'] = jsonpickle.encode(self.cache_storage.partremsize)

    def download(self, gob):
        """call on the storage to retrieve data"""
        fp = None
        
        fp = self.cache_storage.get(getattr(gob, KEYFIELD).value)
        if fp != -1:
            self.store_permanent_records()
            return (gob, fp)
        gob2, iterable = self.perm_storage.download(gob)
        deleted = self.cache_storage.add(getattr(gob, KEYFIELD).value, iterable, getattr(gob, 'size').value)
        
        for elem in deleted:
            del self.tt[elem]

        self.store_permanent_records()
        return (gob, fp)
    
    def upload(self, gob, fp):
        gob2 = self.perm_storage.upload(gob, fp)

    def upload_iter(self, gob, iterable):
        gob2 = self.perm_storage.upload_iter(gob, iterable)

    def search(self, gob):
        """search tokyocabinet for a gob's file handle"""
        try:
            fileid = self.tt[getattr(gob, KEYFIELD).value]
            return 0
        except KeyError:
            return -1
