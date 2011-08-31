from __future__ import absolute_import

from accellion import minifs
from gobpersist import session

import os,sys
import pyrant
import uuid

import jsonpickle

# diskcache.py
# utilizes pyrant, tokyotyrant, tokyocabinet, and minifs.py to
# implement backend storage and caching for user files

KEYFIELD = 'file_id'
"""the File field we want to use to generate keys, accellion/schema.py"""

class TokyoDiskStore(session.StorageEngine):
    """TokyoTyrant client and TokyoCabinet dbm for storage control"""
    def __init__(self, server='127.0.0.1', port=1978, permpath='/home/admin/accellion_filestore', cachepath='/home/admin/accellion_cache', permsize=5000, cachesize=2000, oldconfig=True):

        self.tt = pyrant.Tyrant(server, port)
        """tokyo cabinet client"""

        self.filerecord = []
        self.sizerecord = {}
        self.filerecord_cache = []
        self.sizerecord_cache = {}
        self.storage_spaceleft = 0
        self.cache_spaceleft = 0

        if oldconfig == True:
            try:
                self.filerecord = jsonpickle.decode(self.tt['filerec'])
                self.sizerecord = jsonpickle.decode(self.tt['sizerec'])
                self.filerecord_cache = jsonpickle.decode(self.tt['filerec_cache'])
                self.sizerecord_cache = jsonpickle.decode(self.tt['sizerec_cache'])
                self.cache_spaceleft = jsonpickle.decode(self.tt['cachespace'])
                self.storage_spaceleft = jsonpickle.decode(self.tt['storespace'])
            except KeyError:
                print 'this is the first time setting up this storage engine.'
                self.filerecord = -1
                
        self.filestore_directory = permpath
        self.filecache_directory = cachepath
        """directories for storage"""

        self.filestore_size = permsize
        self.filecache_size = cachesize
        """sizes of storage containers"""

        self.perm_storage = minifs.Partition(self.filestore_directory, self.filestore_size)
        self.cache_storage = minifs.MRUPreserve(self.filecache_directory, self.filecache_size)
        """temporary and permanent disk storage"""
        
        
        if self.filerecord != -1:
            self.perm_storage.recordregistry = self.filerecord
            self.perm_storage.sizeregistry = self.sizerecord
            self.cache_storage.recordregistry = self.filerecord_cache
            self.cache_storage.sizeregistry = self.sizerecord_cache
            self.perm_storage.partremsize = self.storage_spaceleft
            self.cache_storage.partremsize = self.cache_spaceleft

    def store_permanent_records(self):
        """in case of a system halt, recall saved file information"""
        self.tt['filerec'] = jsonpickle.encode(self.perm_storage.recordregistry)
        self.tt['sizerec'] = jsonpickle.encode(self.perm_storage.sizeregistry)
        self.tt['filerec_cache'] = jsonpickle.encode(self.cache_storage.recordregistry)
        self.tt['sizerec_cache'] = jsonpickle.encode(self.cache_storage.sizeregistry)
        self.tt['cachespace'] = jsonpickle.encode(self.cache_storage.partremsize)
        self.tt['storespace'] = jsonpickle.encode(self.perm_storage.partremsize)

    def upload(self, gob, fp):
        """call on the storage to upload data"""
        rslt = self.search(gob) #temp variable to hold method results
        if rslt == 0:
            rslt = self.cache_storage.update(getattr(gob, KEYFIELD).value, fp)
            if rslt == -1:
                rslt = self.perm_storage.get(getattr(gob, KEYFIELD).value)
                if rslt != -1: self.cache_storage.add(getattr(gob, KEYFIELD).value, fp)
                self.tt[getattr(gob, KEYFIELD).value] = getattr(gob, KEYFIELD).value
                self.store_permanent_records()
                return gob
            del self.tt[getattr(gob, KEYFIELD).value] 
            self.tt[getattr(gob, KEYFIELD).value] = getattr(gob, KEYFIELD).value
        rslt = self.perm_storage.add(getattr(gob, KEYFIELD).value, fp)
        if rslt != -1: self.cache_storage.add(getattr(gob, KEYFIELD).value, fp)
        self.tt[getattr(gob, KEYFIELD).value] = getattr(gob, KEYFIELD).value
        self.store_permanent_records()
        return gob
            
    def download(self, gob):
        """call on the storage to retrieve data"""
        fp = None
        searchrslt = self.search(gob)
        if searchrslt == 0:
            fp = self.cache_storage.get(getattr(gob, KEYFIELD).value)
            if fp != -1:
                self.store_permanent_records()
                return (gob, fp)
            fp = self.perm_storage.get(getattr(gob, KEYFIELD).value)
            self.cache_storage.add(getattr(gob, KEYFIELD).value, fp)
            self.store_permanent_records()
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
                self.store_permanent_records()
                return 0
            self.perm_storage.remove(getattr(gob, KEYFIELD).value)
            self.store_permanent_records()
            return 0
        return -1     

    def search(self, gob):
        """search tokyocabinet for a gob's file handle"""
        try:
            fileid = self.tt[getattr(gob, KEYFIELD).value]
            return 0
        except KeyError:
            return -1
