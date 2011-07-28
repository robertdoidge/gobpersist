from __future__ import absolute_import
from .. import session
from .. import exception

from copy import deepcopy

class Cache(session.Backend):
    """A generic cache class, using one backend for cache and one for
    nonvolatile storage.

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
        self.cache = cache
        """The backend which is to operate as a cache."""

        self.cache.caller = self

        super(Cache, self).__init__(backend)


    def query(self, path, query=None, retrieve=None,
              order=None, offset=None, limit=None):
        while True:
            try:
                try:
                    old_backend = self.backend
                    self.backend = self.cache
                    query_f = getattr(self.cache, 'query', self.caller._query)
                    return query_f(path, query, retrieve,
                                   order, offset, limit)
                finally:
                    self.backend = old_backend
            except exception.NotFound:
                # couldn't find it in cache
                cache_refill = getattr(self.backend, 'cache_refill', None)
                if cache_refill is not None:
                    cache_refill(self.cache, path, query,
                                 retrieve, order, offset, limit)
                else:
                    query_f = getattr(self.backend, 'query', self.caller._query)
                    res = query_f(path, query, retrieve,
                                  order, offset, limit)
                    print "OK, got result, now saving it in cache..."
                    try:
                        print "swapping backends..."
                        old_backend = self.backend
                        self.backend = self.cache
                        try:
                            #self.caller.start_transaction()
                            print "iterating over items..."
                            if len(res) == 0:
                                try:
                                    self.caller.add_collection(path)
                                except NotImplementedError:
                                    pass
                            else:
                                for item in res:
                                    # (try to) add it to the cache
                                    self.caller.add(item)
                            print "committing changes..."
                            self.caller.commit()
                        except:
                            #self.caller.rollback()
                            raise
                    finally:
                        self.backend = old_backend
                    return res

    def commit(self):
        if self.backend == self.cache:
            return self.caller._commit()
        gob_invalidate = set()
        for coll in self.caller.operations.itervalues():
            gob_invalidate.update(coll)
        collection_invalidate = set()
        for coll in self.caller.operations.itervalues():
            for gob in coll:
                for path in gob.keys:
                    collection_invalidate.add(path)
        self.caller._commit()
        try:
            old_backend = self.backend
            self.backend = self.cache
            try:
                for item in gob_invalidate:
                    print repr(item)
                    self.caller.remove(item)
                try:
                    for path in collection_invalidate:
                        print repr(path)
                        self.caller.remove_collection(path)
                except NotImplementedError:
                    pass
                self.caller._commit()
            except:
                self.caller.rollback()
                raise
        finally:
            self.backend = old_backend
