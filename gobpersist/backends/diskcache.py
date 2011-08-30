from __future__ import absolute_import

from accellion import minifs
from gobpersist import field
from gobpersist import session

import os,sys
import pyrant
import uuid

import cPickle as pickle

# diskcache.py
# utilizes pyrant, tokyotyrant, tokyocabinet, and minifs.py to
# implement backend storage and caching for user files

KEYFIELD = 'file_id'
"""the File field we want to use to generate keys, accellion/schema.py"""

class TokyoDiskStore(session.StorageEngine):
    """TokyoTyrant client and TokyoCabinet dbm for storage control"""
    def __init__(self, server='127.0.0.1', port=1978, permpath='/home/admin/accellion_filestore', cachepath='/home/admin/accellion_cache', permsize=5000, cachesize=2000):

        self.tt = pyrant.Tyrant(server, port)
        """tokyo cabinet client"""

        self.filestore_directory = permpath
        self.filecache_directory = cachepath
        """directories for storage"""

        self.filestore_size = permsize
        self.filecache_size = cachesize
        """sizes of storage containers"""

        self.perm_storage = minifs.Partition(self.filestore_directory, permsize)
        self.cache_storage = minifs.MRUPreserve(self.filecache_directory, cachesize)
        """temporary and permanent disk storage"""

    def upload(self, gob, fp):
        """call on the storage to upload data"""
        searchrslt = self.search(gob)
        if searchrslt == 0:
            searchrslt = self.cache_storage.update(getattr(gob, KEYFIELD).value, fp)
            if searchrslt == -1:
                searchrslt = self.perm_storage.get(getattr(gob, KEYFIELD).value, fp)
                self.cache_storage.add(getattr(gob, KEYFIELD).value, fp)
                return gob
        self.perm_storage.add(getattr(gob, KEYFIELD).value, fp)
        if searchrslt == 0:
            self.tt[getattr(gob, KEYFIELD).value] = getattr(gob, KEYFIELD).value
        return gob
            
    def download(self, gob):
        """call on the storage to retrieve data"""
        fp = None
        searchrslt = self.search(gob)
        if searchrslt == 0:
            fp = self.cache_storage.get(getattr(gob, KEYFIELD).value)
            if fp != -1:
                return (gob, fp)
            fp = self.perm_storage.get(getattr(gob, KEYFIELD).value)
            self.cache_storage.add(getattr(gob, KEYFIELD).value, fp)
            return (gob, fp)
        return -1
    
    def remove(self, gob):
        """manually remove a file from storage"""
        searchrslt = self.search(gob)
        if searchrslt == 0:
            del self.tt[getattr(gob, KEYFIELD).value]
            searchrslt = self.cache_storage.get(getattr(gob, KEYFIELD).value)
            if searchrslt != -1:
                self.cache_storage.remove(getattr(gob, KEYFIELD).value)
                self.perm_storage.remove(getattr(gob, KEYFIELD).value)
                return 0
            self.perm_storage.remove(getattr(gob, KEYFIELD).value)
            return 0
        return -1     

    def search(self, gob):
        """search tokyocabinet for a gob's file handle"""
        try:
            fileid = self.tt[getattr(gob, KEYFIELD).value]
            return 0
        except KeyError:
            return -1
