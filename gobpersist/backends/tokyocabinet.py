from __future__ import absolute_import

from gobpersist.backends import minifs, cache
from gobpersist import session

import os,sys
import pyrant
import uuid

import jsonpickle

# tokyocabinet.py
# utilizes pyrant, tokyotyrant, tokyocabinet, and minifs.py to
# implement backend storage caching for user files

KEYFIELD = 'file_handle'
"""the File field we want to use to generate keys, accellion/schema.py"""

default_pool = cache.SimpleThreadMappedPool(client=pyrant.Tyrant)

def generate_key(gob):
    newkey = getattr(gob, KEYFIELD).value
    newkey = newkey.replace('/', '')
    return newkey

class TokyoCabinetBackend(session.StorageEngine):
    """TokyoTyrant client and TokyoCabinet dbm for storage control"""
    def __init__(self, server='127.0.0.1', port=1978, cachepath='accellion_cache', cachesize=2000,
                 pool=default_pool):
        
        self.tt_args = ()
        self.tt_kwargs = {'host':server, 'port':port}
        self.pool = pool

        """tokyo cabinet client"""
	self.cache_space = 0
        self.filerecord_cache = []
        self.sizerecord_cache = {}
        self.cache_space_left = 0

        self.filecache_directory = cachepath
        """directories for storage"""

        self.filecache_size = cachesize
        """sizes of storage container"""

        self.cache_storage = minifs.MRUGobPreserve(self.filecache_directory, self.filecache_size)
        """temporary disk storage"""        

        with self.pool.reserve(*self.tt_args, **self.tt_kwargs) as tt:
            try:
	    	self.cache_space = jsonpickle.decode(tt['cache_space'])
                self.filerecord_cache = jsonpickle.decode(tt['filerec_cache'])
                self.sizerecord_cache = jsonpickle.decode(tt['sizerec_cache'])
                self.cache_space_left = jsonpickle.decode(tt['cache_space_left'])
            except KeyError:
                #FIXME: should we be storing the default params in TC to avoid another key miss?
                self.clean_directory()
                self.filerecord_cache = -1
                        
        if self.filerecord_cache != -1:
	    if cachesize != self.cache_space:
	        self.cache_space = cachesize
	    self.cache_storage.partsize = self.cache_space
            self.cache_storage.recordregistry = self.filerecord_cache
            self.cache_storage.sizeregistry = self.sizerecord_cache
            self.cache_storage.partremsize = self.cache_space_left

    def clean_directory(self):
        for elem in os.listdir(self.filecache_directory):
            path = os.path.join(self.filecache_directory.encode('utf-8'), elem)
            try:
                os.unlink(path)
            except Exception, e:
                print e

    def search(self, gob):
        """search tokyocabinet for a gob's file handle"""
        with self.pool.reserve(*self.tt_args, **self.tt_kwargs) as tt:
            try:
                fileid = tt[getattr(gob, KEYFIELD).value]
                return 0
            except KeyError:
                return -1

class TokyoCabinetCache(TokyoCabinetBackend, session.StorageEngine):
    
    def __init__(self, server='127.0.0.1', port=1978, cachepath='accellion_cache', cachesize=2000):    
        super(TokyoCabinetCache, self).__init__(server, port, cachepath, cachesize)
        
    def remove_deleted(self, deleted):
        with self.pool.reserve(*self.tt_args, **self.tt_kwargs) as tt:
            for elem in deleted:
                del tt[elem]
    
    def store_permanent_records(self):
        """in case of a system halt, recall saved file information"""
        with self.pool.reserve(*self.tt_args, **self.tt_kwargs) as tt:
            tt['filerec_cache'] = jsonpickle.encode(self.cache_storage.recordregistry)
            tt['sizerec_cache'] = jsonpickle.encode(self.cache_storage.sizeregistry)
            tt['cache_space_left'] = jsonpickle.encode(self.cache_storage.partremsize)
    
    def download(self, gob):
        """call on the storage to retrieve data"""
        iterable = None
        
        file_identifier = generate_key(gob)

        iterable = self.cache_storage.get(file_identifier)
        
        if iterable != -1:
            self.store_permanent_records()
            return (gob, iterable)

        return(gob, -1)
    
    def upload(self, gob, iterable):
        
        deleted = self.cache_storage.add(generate_key(gob), iterable, getattr(gob, 'size').value)
        self.remove_deleted(deleted)

        self.store_permanent_records()
        return gob

     
