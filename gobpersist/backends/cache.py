# cache.py - A generic caching back end
# Copyright (C) 2012 Accellion, Inc.
#
# This library is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation; version 2.1.
#
# This library is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301 USA
"""Abstract and proxy classes for database caching support.

.. moduleauthor:: Evan Buswell <evan.buswell@accellion.com>
"""

import itertools

import gobpersist.session
import gobpersist.exception

class CachingBackend(gobpersist.session.Backend):
    """A generic caching back end, using any cache back end and any
    back end for nonvolatile storage.

    By default this class will manage cache misses by simply passing
    the request on to the back end and storing the retrieved value in
    the cache.  However, if the back end wishes to prefill caches, it
    can define a method, ``cache_refill``, taking the same parameters
    as :func:`Cache.query`, that will manage the
    cache refill.  After the cache has been refilled, the object
    should be available in the cache, although cache invalidation may
    cause the next request to fail.  In that case, the caching query
    will loop and try to call ``cache_refill`` again.
    """
    def __init__(self, cache, backend):
        """
        Args:
           ``cache``: The back end which is to operate as a cache.

           ``backend``: The "real" back end for the cache.
        """
        self.backend = backend
        """The "real" back end for the cache."""
        self.cache = cache
        """The back end which is to operate as a cache."""


    def query(self, cls, key=None, key_range=None, query=None, retrieve=None,
              order=None, offset=None, limit=None):
        while True:
            try:
                return self.cache.query(cls, key, key_range, query,
                                        retrieve, order, offset, limit)
            except gobpersist.exception.NotFound:
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


class Cache(gobpersist.session.GobTranslator):
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

