from __future__ import absolute_import
import itertools
import thread
import contextlib
import time

from .. import session
from .. import exception

class CachingBackend(session.Backend):
    """A generic caching back end, using any cache backend and any
    backend for nonvolatile storage.

    By default this class will manage cache misses by simply passing
    the request on to the back end and storing the retrieved value in
    the cache.  However, if the backend wishes to prefill caches, it
    can define a method, 'cache_refill', taking the same parameters as
    query, that will manage the cache refill.  After the cache has
    been refilled, the object should be available in the cache,
    although cache invalidation may cause the next request to fail.
    In that case, the caching query will loop and try to call
    cache_refill again.
    """
    def __init__(self, cache, backend):
        self.backend = backend
        """The "real" backend for the cache."""
        self.cache = cache
        """The backend which is to operate as a cache."""


    def query(self, cls, key=None, key_range=None, query=None, retrieve=None,
              order=None, offset=None, limit=None):
        while True:
            try:
                return self.cache.query(cls, key, key_range, query,
                                        retrieve, order, offset, limit)
            except exception.NotFound:
                # couldn't find it in cache
                cache_refill = getattr(self.backend, 'cache_refill', None)
                if cache_refill is not None:
                    cache_refill(self.cache, cls, key, key_range, query,
                                 retrieve, order, offset, limit)
                else:
                    res = self.backend.query(cls, key, key_range, query,
                                             retrieve, order, offset,
                                             limit)
                    self.cache.cache_query(cls, res, key, key_range, query,
                                           retrieve, order, offset,
                                           limit)
                    # return res

    def commit(self, additions=[], updates=[], removals=[],
               collection_additions=[], collection_removals=[]):
        gob_invalidate = []
        key_invalidate = set(collection_additions)
        key_invalidate.update(collection_removals)
        for op in itertools.chain(additions, updates, removals):
            if 'remove_unique_keys' in op:
                key_invalidate.update(op['remove_unique_keys'])
            if 'add_unique_keys' in op:
                key_invalidate.update(op['add_unique_keys'])
            if 'add_keys' in op:
                key_invalidate.update(op['add_keys'])
            if 'remove_keys' in op:
                key_invalidate.update(op['remove_keys'])
            gob_invalidate.append(op['gob'])

        ret = self.backend.commit(additions, updates, removals,
                                  collection_additions,
                                  collection_removals)
        self.cache.invalidate(gob_invalidate, key_invalidate)
        return ret


class Cache(session.GobTranslator):
    """Abstract superclass for cache implementations."""

    def query(self, cls, key=None, key_range=None, query=None, retrieve=None,
              order=None, offset=None, limit=None):
        """Query the cache."""
        raise NotImplementedError("Cache type '%s' does not implement" \
                                      " query" % self.__class__.__name__)

    def cache_query(self, cls, items, key=None, key_range=None, query=None,
                    retrieve=None, order=None, offset=None, limit=None):
        """Store the results of a query in the cache."""
        raise NotImplementedError("Cache type '%s' does not implement" \
                                      " cache_query" % self.__class__.__name__)

    def cache_items(self, items):
        """Store the provided items in the cache."""
        raise NotImplementedError("Cache type '%s' does not implement" \
                                      " cache" % self.__class__.__name__)

    def invalidate(self, items=None, keys=None):
        """Invalidate all queries pertaining to these objects."""
        raise NotImplementedError("Cache type '%s' does not implement" \
                                      " invalidate" % self.__class__.__name__)


class SimpleThreadMappedPool(object):
    def __init__(self, client):
        '''
        @param client: The class of the client object.  We will be instantiating these based on the
                        args and kwargs passed in to reserve()
        '''
        self.pool = {}
        self.client = client

    @contextlib.contextmanager
    def reserve(self, *args, **kwargs):
        thread_id = thread.get_ident()
        if thread_id not in self.pool:
            # create a new Client
            client_hash = self.pool[thread_id] = {}
            client_hash['args'] = args
            client_hash['kwargs'] = kwargs
            client_hash['client'] = self.client(*args, **kwargs)
            client_hash['time'] = time.time()
            yield client_hash['client']
        else:
            client_hash = self.pool[thread_id]
            if client_hash['args'] != args or client_hash['kwargs'] != kwargs \
                    or ('client' in client_hash and not client_hash['client']) \
                    or client_hash['time'] > time.time() + 60 * 3:
                #Try to close the existing connection using any method we know about
                client_hash['client'] = self.client_close(client_hash['client'])
                client_hash['args'] = args
                client_hash['kwargs'] = kwargs
                client_hash['client'] = self.client(*args, **kwargs)
                client_hash['time'] = time.time()
                yield client_hash['client']
            else:
                yield client_hash['client']

    def relinquish(self):
        thread_id = thread.get_ident()
        if thread_id in self.pool:
            client_hash = self.pool[thread_id]
            client_hash['client'] = self.client_close(client_hash['client'])
            del self.pool[thread_id]

    def client_close(self, client):
        #Try to close the existing connection using any method we know about
        if hasattr(client, 'disconnect_all'):
            client.disconnect_all()
        else: #Or just drop the object all together.
            del client
            client = None
            
        return client