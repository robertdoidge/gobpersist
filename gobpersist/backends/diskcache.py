from __future__ import absolute_import

from accellion import minifs
from gobpersist import field
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
    def __init__(self, server='127.0.0.1', port=1978, permpath='/home/admin/accellion_filestore', cachepath='/home/admin/accellion_cache', permsize=5000, cachesize=2000, oldconfig=False):

        self.tt = pyrant.Tyrant(server, port)
        """tokyo cabinet client"""

        self.filerecord = []
        self.sizerecord = {}

        if oldconfig == True:
            try:
                self.filerecord = jsonpickle.decode(self.tt['filerec'])
                self.sizerecord = jsonpickle.decode(self.tt['sizerec'])
            except KeyError:
                print 'bummer'

        self.filestore_directory = permpath
        self.filecache_directory = cachepath
        """directories for storage"""

        self.filestore_size = permsize
        self.filecache_size = cachesize
        """sizes of storage containers"""

        self.perm_storage = minifs.Partition(self.filestore_directory, permsize)
        self.cache_storage = minifs.MRUPreserve(self.filecache_directory, cachesize)
        """temporary and permanent disk storage"""

        if self.filerecord is not -1:
            self.perm_storage.recordregistry = self.filerecord
            self.perm_storage.sizeregistry = self.sizerecord

    def key_removal(self, del_list):
        for key in del_list:
            del self.tt[key]

    def store_permanent_records(self):
        """in case of a system halt, recall saved file information"""
        self.tt['filerec'] = jsonpickle.encode(self.perm_storage.recordregistry)
        self.tt['sizerec'] = jsonpickle.encode(self.perm_storage.sizeregistry)

    def upload(self, gob, fp):
        """call on the storage to upload data"""
        searchrslt = self.search(gob)
        if searchrslt == 0:
            searchrslt, del_list = self.cache_storage.update(getattr(gob, KEYFIELD).value, fp)
            if del_list is not None:
                self.key_removal(del_list)
            if searchrslt == -1:
                searchrslt = self.perm_storage.get(getattr(gob, KEYFIELD).value)
                self.key_removal(self.cache_storage.add(getattr(gob, KEYFIELD).value, fp))
                self.tt[getattr(gob, KEYFIELD).value] = getattr(gob, KEYFIELD).value
                self.store_permanent_records()
                return gob
            del self.tt[getattr(gob, KEYFIELD).value] 
            self.tt[getattr(gob, KEYFIELD).value] = getattr(gob, KEYFIELD).value
        rslt = self.perm_storage.add(getattr(gob, KEYFIELD).value, fp)
        if rslt != -1: self.key_removal(self.cache_storage.add(getattr(gob, KEYFIELD).value, fp))
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
            self.key_removal(self.cache_storage.add(getattr(gob, KEYFIELD).value, fp))
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
            print 'found it!!!'
            return 0
        except KeyError:
            return -1
