from __future__ import absolute_import
import itertools

from .. import session
from .. import exception

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
        self.backend = backend
        """The "real" backend for the cache."""
        self.cache = cache
        """The backend which is to operate as a cache."""


    def query(self, path=None, path_range=None, query=None, retrieve=None,
              order=None, offset=None, limit=None):
        while True:
            try:
                return self.cache.query(path, path_range, query, retrieve,
                                        order, offset, limit)
            except exception.NotFound:
                # couldn't find it in cache
                cache_refill = getattr(self.backend, 'cache_refill', None)
                if cache_refill is not None:
                    cache_refill(self.cache, path, path_range, query,
                                 retrieve, order, offset, limit)
                else:
                    res = self.backend.query(path, path_range, query, retrieve,
                                             order, offset, limit)
                    print "OK, got result, now saving it in cache..."
                    print "iterating over items..."
                    if len(res) == 0:
                        self.cache.commit(collection_additions=[path])
                    else:
                        self.cache.commit(additions=[{'gob': item} \
                                                         for item in res])
                    return res

    def commit(self, additions=[], updates=[], removals=[],
               collection_additions=[], collection_removals=[]):
        gob_invalidate = []
        collection_invalidate = set(collection_additions)
        collection_invalidate.update(collection_removals)
        for op in itertools.chain(additions, updates, removals):
            remove_unique_keys = []
            if 'remove_unique_keys' in op:
                remove_unique_keys.append(op['remove_unique_keys'])
            if 'add_unique_keys' in op:
                remove_unique_keys.append(op['add_unique_keys'])
            new_op = {
                'gob': op['gob'],
                'remove_unique_keys': remove_unique_keys
                }
            gob_invalidate.append(new_op)
            add_keys = op['gob'].keys
            if 'add_keys' in op:
                add_keys = itertools.chain(add_keys, op['add_keys'])
            if 'remove_keys' in op:
                add_keys = itertools.chain(add_keys, op['remove_keys'])

            for path in add_keys:
                collection_invalidate.add(path)

        ret = self.backend.commit(additions, updates, removals,
                            collection_additions, collection_removals)
        self.cache.commit(removals=gob_invalidate,
                          collection_removals=collection_invalidate)
        return ret
