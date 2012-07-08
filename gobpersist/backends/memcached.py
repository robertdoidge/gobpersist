# memcached.py - Back end interface to memcached
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
"""Interface to the memcached cache or anything else which speaks the
memcached protocol.

.. moduleauthor:: Evan Buswell <evan.buswell@accellion.com>
"""

import time
import cPickle as pickle
import datetime
import itertools
import functools

import pylibmc

import gobpersist.backends.gobkvquerent
import gobpersist.backends.pools
import gobpersist.backends.cache
import gobpersist.exception
import gobpersist.field

class PickleWrapper(object):
    loads = pickle.loads

    @staticmethod
    def dumps(obj):
        return pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)

default_pool = gobpersist.backends.pools.SimpleThreadMappedPool(client=pylibmc.Client)

class MemcachedBackend(gobpersist.backends.gobkvquerent.GobKVQuerent):
    """Gob back end which uses memcached for storage"""

    def __init__(self, servers=['127.0.0.1'], expiry=0, binary=True,
                 serializer=PickleWrapper, lock_prefix='_lock',
                 pool=default_pool, separator='.', lock_tries=8,
                 lock_backoff=0.25, *args, **kwargs):
        """
        Args:
           ``servers``: The ``servers`` argument for the memcached
           client -- a list of servers over which to distribute data.

           ``expiry``: The expiry time for all values set during this
           session, in number of seconds.

           ``binary``: Whether to use the binary or the http memcached
           protocol.

           ``serializer``: An object which provides serialization of
           gobpersist data.

              Must provide ``loads`` and ``dumps``.

           ``lock_prefix``: A string to prepend to a key value to
           represent the lock for that key.

           ``pool``: The pool of memcached connections.

           ``separator``: The separator between key elements.

              The default is '.'.

           ``lock_tries``: The number of times to try locking before
           forcibly acquiring all locks.

              Since there's no way to monitor other processes to make
              sure they've properly cleaned up, this prevents a
              permanent object lock by a crashed or hung process.  The
              default is 8.

           ``lock_backoff``: The amount of time, in seconds, for the
           locking mechanism to wait between tries.

              The default is 0.25.  The maximum wait time for any lock
              acquisition is ``lock_tries * lock_backoff``, so
              consider this value when fine-tuning these.
        """
        behaviors = {'ketama': True}
        for key, value in kwargs.iteritems():
            behaviors[key] = value

        self.mc_args = (servers,)
        self.mc_kwargs = {'behaviors': behaviors, 'binary': binary}
        """The arguments for the memcached client."""

        self.pool = pool
        """The pool of memcached connections."""

        self.serializer = serializer
        """An object which provides serialization of gobpersist data.

        Must provide ``loads`` and ``dumps``."""

        self.lock_prefix = lock_prefix
        """A string to prepend to a key value to represent the lock
        for that key."""

        self.expiry = expiry
        """The expiry time for all values set during this session, in
        number of seconds."""

        self.separator = separator
        """The separator between key elements.

        The default is '.'.
        """

        self.lock_tries = lock_tries
        """The number of times to try locking before forcibly
        acquiring all locks.

        Since there's no way to monitor other processes to make sure
        they've properly cleaned up, this prevents a permanent object
        lock by a crashed or hung process.  The default is 8.
        """

        self.lock_backoff = lock_backoff
        """The amount of time, in seconds, for the locking mechanism
        to wait between tries.

        The default is 0.25.  The maximum wait time for any lock
        acquisition is ``lock_tries * lock_backoff``, so consider this
        value when fine-tuning these.
        """

        super(MemcachedBackend, self).__init__()

    def do_kv_multi_query(self, cls, keys):
        keys = [str(self.separator.join(key)) for key in keys]
        with self.pool.reserve(*self.mc_args, **self.mc_kwargs) as mc:
            res = mc.get_multi(keys)
        ret = []
        for key in keys:
            if key not in res:
                raise gobpersist.exception.NotFound(
                    "Could not find value for key %s" \
                        % key)
            store = self.serializer.loads(res[key])
            if isinstance(store, (list, tuple)):
                # Collection or reference?
                if len(store) == 0:
                    # Empty collection
                    ret.append(store)
                elif isinstance(store[0], (list, tuple)):
                    # Collection
                    ret.append(self.do_kv_multi_query(cls, store))
                else:
                    # Reference
                    ret.append(self.do_kv_query(cls, store)[0])
            else:
                # Object
                ret.append(self.mygob_to_gob(cls, store))
        return ret

    def do_kv_query(self, cls, key):
        with self.pool.reserve(*self.mc_args, **self.mc_kwargs) as mc:
            res = mc.get(str(self.separator.join(key)))
        if res == None:
            raise gobpersist.exception.NotFound(
                "Could not find value for key %s" \
                    % self.separator.join(key))
        store = self.serializer.loads(res)
        if isinstance(store, (list, tuple)):
            # Collection or reference?
            if len(store) == 0:
                # Empty collection
                return store
            elif isinstance(store[0], (list, tuple)):
                # Collection
                return self.do_kv_multi_query(cls, store)
            else:
                # Reference
                return self.do_kv_query(cls, store)
        else:
            # Object
            return [self.mygob_to_gob(cls, store)]

    def kv_query(self, cls, key=None, key_range=None):
        if key_range is not None:
            raise gobpersist.exception.UnsupportedError("key_range is not supported by" \
                                                 " memcached")
        return self.do_kv_query(cls, self.key_to_mykey(key))

    def try_acquire_locks(self, locks):
        """Tries to acquire the locks.
        
        Returns true if successful, false otherwise.  Be very careful
        calling this function from outside this module, as the locking
        mechanism does not support holding locks for long periods of
        time.
        """
        locks_acquired = []
        try:
            with self.pool.reserve(*self.mc_args, **self.mc_kwargs) as mc:
                for lock in locks:
                    # Lock the object
                    if mc.add(lock, '1'):
                        locks_acquired.append(lock)
                    else:
                        # The object is locked!
                        # Back out all acquired locks
                        self.release_locks(locks_acquired)
                        return False
            return True
        except:
            self.release_locks(locks_acquired)
            raise

    def acquire_locks(self, locks):
        """Atomically acquires a set of locks.

        Be very careful calling this function from outside this
        module, as the locking mechanism was does not support holding
        locks for long periods of time.
        """
        tries = self.lock_tries
        # After this many tries, we forcibly acquire the locks

        while tries > 0:
            if self.try_acquire_locks(locks):
                return locks
            else:
                # We failed to acquire all the locks; loop and try again
                tries -= 1
                time.sleep(self.lock_backoff)

        # We failed to acquire locks after *tries* attempts.  Say a
        # hail mary and force acquire.
        with self.pool.reserve(*self.mc_args, **self.mc_kwargs) as mc:
            for lock in locks:
                # Hail Mary!
                # Lock the object
                mc.set(lock, 'locked')
            return locks

    def release_locks(self, locks):
        """Releases a set of locks."""
        with self.pool.reserve(*self.mc_args, **self.mc_kwargs) as mc:
            mc.delete_multi(locks)

    def key_to_mykey(self, key, use_persisted_version=False):
        mykey = super(MemcachedBackend, self).key_to_mykey(key,
                                                           use_persisted_version)
        return tuple([keyelem.isoformat() \
                                  if isinstance(keyelem, datetime.datetime) \
                              else '_NULL_' if keyelem is None \
                              else str(keyelem) \
                          for keyelem in mykey])

    def commit(self, additions=[], updates=[], removals=[],
               collection_additions=[], collection_removals=[]):
        # Build the set of commits
        to_set = []
        to_add = []
        to_delete = []
        collection_add = []
        collection_remove = []
        locks = set()
        conditions = []

        for addition in additions:
            gob = addition['gob']
            gob_key = self.key_to_mykey(gob.obj_key)
            locks.add(self.lock_prefix + self.separator + self.separator.join(gob_key))
            to_add.append((gob_key, self.gob_to_mygob(gob)))
            if 'add_unique_keys' in addition:
                add_unique_keys = itertools.chain(
                    gob.unique_keyset(),
                    addition['add_unique_keys'])
            else:
                add_unique_keys = gob.unique_keyset()
            if 'add_keys' in addition:
                add_keys = itertools.chain(
                    gob.keyset(),
                    addition['add_keys'])
            else:
                add_keys = gob.keyset()
            for key in itertools.imap(
                    self.key_to_mykey,
                    add_unique_keys):
                locks.add(self.lock_prefix + self.separator + self.separator.join(key))
                to_add.append((key, gob_key))
            for key in itertools.imap(
                    self.key_to_mykey,
                    add_keys):
                locks.add(self.lock_prefix + self.separator + self.separator.join(key))
                collection_add.append((key, gob_key))

        for update in updates:
            gob = update['gob']
            gob_key = self.key_to_mykey(gob.obj_key)
            locks.add(self.lock_prefix + self.separator + self.separator.join(gob_key))
            to_set.append((gob_key, self.gob_to_mygob(gob)))
            for key in gob.unique_keyset():
                for f in key:
                    if isinstance(f, gobpersist.field.Field) and f.dirty:
                        new_key = self.key_to_mykey(key)
                        old_key = self.key_to_mykey(key, True)
                        locks.add(self.lock_prefix + self.separator + self.separator.join(new_key))
                        locks.add(self.lock_prefix + self.separator + self.separator.join(old_key))
                        to_delete.append(old_key)
                        to_add.append((new_key, gob_key))
                        break
            for key in gob.keyset():
                for f in key:
                    if isinstance(f, gobpersist.field.Field) and f.dirty:
                        new_key = self.key_to_mykey(key)
                        old_key = self.key_to_mykey(key, True)
                        locks.add(self.lock_prefix + self.separator + self.separator.join(new_key))
                        locks.add(self.lock_prefix + self.separator + self.separator.join(old_key))
                        collection_remove.append((old_key, gob_key))
                        collection_add.append((new_key, gob_key))
                        break
            if 'add_keys' in update:
                for key in itertools.imap(self.key_to_mykey,
                                           update['add_keys']):
                    locks.add(self.lock_prefix + self.separator + self.separator.join(key))
                    collection_add.append((key, gob_key))
            if 'add_unique_keys' in update:
                for key in itertools.imap(self.key_to_mykey,
                                           update['add_unique_keys']):
                    locks.add(self.lock_prefix + self.separator + self.separator.join(key))
                    to_add.append((key, gob_key))
            if 'remove_keys' in update:
                for key in itertools.imap(self.key_to_mykey,
                                           update['remove_keys']):
                    locks.add(self.lock_prefix + self.separator + self.separator.join(key))
                    collection_remove.append((key, gob_key))
            if 'remove_unique_keys' in update:
                for key in itertools.imap(self.key_to_mykey,
                                           update['remove_unique_keys']):
                    locks.add(self.lock_prefix + self.separator + self.separator.join(key))
                    to_delete.append(key)

        for removal in removals:
            gob = removal['gob']
            gob_key = self.key_to_mykey(gob.obj_key)
            locks.add(self.lock_prefix + self.separator + self.separator.join(gob_key))
            to_delete.append(gob_key)
            if 'remove_keys' in removal:
                for key in itertools.imap(self.key_to_mykey,
                                          removal['remove_keys']):
                    locks.add(self.lock_prefix + self.separator + self.separator.join(key))
                    collection_remove.append((key, gob_key))
            if 'remove_unique_keys' in removal:
                for key in itertools.imap(self.key_to_mykey,
                                          removal['remove_unique_keys']):
                    locks.add(self.lock_prefix + self.separator + self.separator.join(key))
                    to_delete.append(key)
            for key in itertools.imap(lambda x: self.key_to_mykey(x, True),
                                      gob.keyset()):
                locks.add(self.lock_prefix + self.separator + self.separator.join(key))
                collection_remove.append((key, gob_key))
            for key in itertools.imap(lambda x: self.key_to_mykey(x, True),
                                      gob.unique_keys):
                locks.add(self.lock_prefix + self.separator + self.separator.join(key))
                to_delete.append(key)

        for key in itertools.imap(self.key_to_mykey, collection_additions):
            locks.add(self.lock_prefix + self.separator + self.separator.join(key))
            to_add.append((key, []))

        for key in itertools.imap(self.key_to_mykey, collection_removals):
            locks.add(self.lock_prefix + self.separator + self.separator.join(key))
            to_delete.append(key)

        # Acquire locks
        self.acquire_locks(locks)
        try:
            
            # Check all conditions
            for alteration in itertools.chain(updates, removals):
                if 'conditions' in alteration:
                    try:
                        res = self.kv_query(alteration['gob'].__class__,
                                            alteration['gob'].obj_key)
                        if len(res) == 0:
                            # A collection instead of an object??
                            # This indicates some kind of corruption...
                            raise gobpersist.exception.Corruption(
                                "Key %s indicates an empty collection" \
                                    " instead of an object." \
                                    % repr(alteration['gob'].obj_key))
                        gob = res[0]
                        if not self._execute_query(gob,
                                                   alteration['conditions']):
                            raise gobpersist.exception.ConditionFailed(
                                "The conditions '%s' could not be met for" \
                                    " object '%s'" \
                                    % (repr(alteration['conditions']),
                                       repr(gob)))
                    except gobpersist.exception.NotFound:
                        # Since this is memcached, we should be
                        # lax about missing values
                        # raise gobpersist.exception.ConditionFailed(
                        #     "The conditions '%s' could not be met for" \
                        #         " object '%s', as the object could not be found" \
                        #         % (repr(alteration['conditions']),
                        #            repr(alteration['gob'].obj_key)))
                        continue

            # Conditions pass! Actually perform the actions
            # print "to_set:", to_set, "to_add:", to_add, \
            #     "to_delete:", to_delete, "collection_add:", collection_add, \
            #     "collection_remove:", collection_remove, \
            #     "locks:", locks, "conditions:", conditions

            with self.pool.reserve(*self.mc_args, **self.mc_kwargs) as mc:
                add_multi = {}
                for add in to_add:
                    add_multi[self.separator.join(add[0])] = self.serializer.dumps(add[1])
                # no add_multi??
                mc.set_multi(add_multi, self.expiry)
                c_addsrms = mc.get_multi([self.separator.join(c_add[0]) \
                                                   for c_add \
                                                   in itertools.chain(
                                                       collection_add,
                                                       collection_remove)])
                for key in c_addsrms:
                    c_addsrms[key] \
                        = set([tuple(path)
                               for path in self.serializer.loads(c_addsrms[key])])
                for c_add in collection_add:
                    key = self.separator.join(c_add[0])
                    if key in c_addsrms:
                        res = c_addsrms[key]
                    else:
                        res = c_addsrms[key] = set()
                    res.add(c_add[1])
                for c_rm in collection_remove:
                    key = self.separator.join(c_add[0])
                    if key in c_addsrms:
                        res = c_addsrms[key]
                        res.discard(c_rm[1])
                set_multi = {}
                for k, v in c_addsrms.iteritems():
                    set_multi[k] = self.serializer.dumps(list(v))
                for setting in to_set:
                    set_multi[self.separator.join(setting[0])] \
                        = self.serializer.dumps(setting[1])
                mc.set_multi(set_multi, self.expiry)
                mc.delete_multi([self.separator.join(delete) for delete in to_delete])
        finally:
            # Done.  Release the locks.
            self.release_locks(locks)
        # memcached never changes items on update
        return []


class MemcachedCache(MemcachedBackend, gobpersist.backends.cache.Cache):
    """A cache backend based on Memcached."""
    def __init__(self, servers=['127.0.0.1'], expiry=0, binary=True,
                 serializer=PickleWrapper, lock_prefix='_lock',
                 pool=default_pool, separator='.', lock_tries=8,
                 lock_backoff=0.25, integrity_prefix='_INTEGRITY_',
                 shadow_prefix='_SHADOW_',
                 *args, **kwargs):
        """
        Args:
           ``integrity_prefix``: A prefix to add to a key to create
           the integrity key for that key.

              Integrity keys keep lists of cached keys where when the
              value of the key is changed, the cached keys should be
              invalidated.

           ``shadow_prefix``: A prefix to add to a key to create the shadow key for
           that key.

             Shadow keys are the keys representing everything
             currently in the cache, rather than in whatever data
             source the cache represents.
        """

        self.integrity_prefix = integrity_prefix
        """A prefix to add to a key to create the integrity key for
        that key.

        Integrity keys keep lists of cached keys where when the value
        of the key is changed, the cached keys should be invalidated.
        """

        self.shadow_prefix = shadow_prefix
        """A prefix to add to a key to create the shadow key for
        that key.

        Shadow keys are the keys representing everything currently in
        the cache, rather than in whatever data source the cache
        represents.
        """

        MemcachedBackend.__init__(self, servers, expiry, binary, serializer,
                                  lock_prefix, pool, separator, lock_tries,
                                  lock_backoff)

    def query(self, cls, key=None, key_range=None, query=None, retrieve=None,
              order=None, offset=None, limit=None):
        # go from least to most limited.
        base_key = key
        if key_range is not None:
            if key is not None:
                raise ValueError("Both key and key_range specified")
            base_key = self._key_range_to_key(key_range)
        try:
            return super(MemcachedCache, self).query(
                cls=cls,
                key=base_key,
                query=query,
                retrieve=retrieve,
                order=order,
                offset=offset,
                limit=limit)
        except gobpersist.exception.NotFound:
            pass
        # didn't find a general query; is there a more specific query?
        # Order is significant...
        if query is not None:
            base_key = self._query_to_key(base_key, query)
            try:
                return super(MemcachedCache, self).query(
                    cls=cls,
                    key=base_key,
                    retrieve=retrieve,
                    order=order,
                    offset=offset,
                    limit=limit)
            except gobpersist.exception.NotFound:
                pass
        if retrieve is not None:
            base_key = self._retrieve_to_key(base_key, retrieve)
            try:
                return super(MemcachedCache, self).query(
                    cls=cls,
                    key=base_key,
                    order=order,
                    offset=offset,
                    limit=limit)
            except gobpersist.exception.NotFound:
                pass
        if offset is not None or limit is not None:
            base_key = self._offlim_to_key(base_key, offset, limit)
            return super(MemcachedCache, self).query(
                cls=cls,
                key=base_key,
                order=order)
        raise gobpersist.exception.NotFound(
            "Could not find value for key %s" \
                % self.separator.join(self.key_to_mykey(base_key)))


    def do_cache_query(self, items, base_key=None):

        # we add the following entries:
        # 1. Each entry is added as the result of base_key
        # 2. All unique keys for this object are added
        # 3. Base_key is added to (integrity_prefix, key) for each key
        # 4. Each entry's keys and unique keys are added as shadow keys

        to_set = {}
        integrity_add = []
        integrity_cascade = []
        shadow_add = []
        locks = set()
        if base_key is not None:
            if len(items) == 1 and items[0].obj_key == base_key:
                # this was a query on a primary key
                # ...or something is (and subsequently will be...)
                # horribly wrong
                base_key = None
            else:
                base_key = self.key_to_mykey(base_key)
                base_coll = []
                locks.add(self.lock_prefix + self.separator +
                          self.separator.join(base_key))
        for gob in items:
            gob_key = self.key_to_mykey(gob.obj_key)
            to_set[self.separator.join(gob_key)] \
                = self.serializer.dumps(self.gob_to_mygob(gob))
            locks.add(self.lock_prefix + self.separator + self.separator.join(gob_key))
            for key in itertools.imap(
                    self.key_to_mykey,
                    gob.unique_keyset()):
                locks.add(self.lock_prefix + self.separator + self.separator.join(key))
                to_set[self.separator.join(key)] = self.serializer.dumps(gob_key)
                locks.add(self.lock_prefix + self.separator + self.shadow_prefix + self.separator + self.separator.join(key))
                to_set[self.shadow_prefix + self.separator + self.separator.join(key)] = self.serializer.dumps(gob_key)
            for key in itertools.imap(
                    self.key_to_mykey,
                    gob.keyset()):
                locks.add(self.lock_prefix + self.separator + self.shadow_prefix + self.separator + self.separator.join(key))
                shadow_add.append(((self.shadow_prefix,) + key, gob_key))
            if base_key is not None:
                # Don't try to set integrity if there isn't a base_key
                for key in itertools.imap(
                        self.key_to_mykey,
                        gob.keyset()):
                    key = (self.integrity_prefix,) + key
                    locks.add(self.lock_prefix + self.separator + self.separator.join(key))
                    integrity_add.append((key, base_key))
                base_coll.append(gob_key)

        if base_key is not None:
            to_set[self.separator.join(base_key)] = self.serializer.dumps(base_coll)

        self.acquire_locks(locks)
        try:
            with self.pool.reserve(*self.mc_args, **self.mc_kwargs) as mc:
                # integrity adds
                if integrity_add:
                    c_adds = mc.get_multi([self.separator.join(c_add[0]) \
                                               for c_add \
                                               in integrity_add])
                    for key in c_adds:
                        c_adds[key] \
                            = set([tuple(path)
                                   for path in self.serializer.loads(c_adds[key])])
                    for c_add in integrity_add:
                        key = self.separator.join(c_add[0])
                        if key in c_adds:
                            res = c_adds[key]
                        else:
                            res = c_adds[key] = set()
                        res.add(c_add[1])
                    integrity_set = {}
                    for k,v in c_adds.iteritems():
                        integrity_set[k] = self.serializer.dumps(list(v))

                # shadow adds
                c_adds = mc.get_multi([self.separator.join(c_add[0]) \
                                           for c_add \
                                           in shadow_add])
                for key in c_adds:
                    c_adds[key] \
                        = set([tuple(path)
                               for path in self.serializer.loads(c_adds[key])])
                for c_add in shadow_add:
                    key = self.separator.join(c_add[0])
                    if key in c_adds:
                        res = c_adds[key]
                    else:
                        res = c_adds[key] = set()
                    res.add(c_add[1])
                shadow_set = {}
                for k,v in c_adds.iteritems():
                    shadow_set[k] = self.serializer.dumps(list(v))

                # Cache and unique keys
                mc.set_multi(to_set, self.expiry)

                # finish up integrity and shadow
                if self.expiry > 0:
                    mc.set_multi(shadow_set, self.expiry + 10)
                else:
                    mc.set_multi(shadow_set, self.expiry)
                if integrity_add:
                    if self.expiry > 0:
                        mc.set_multi(integrity_set, self.expiry + 10)
                    else:
                        mc.set_multi(integrity_set, self.expiry)
        finally:
            self.release_locks(locks)


    def cache_query(self, cls, items, key=None, key_range=None, query=None,
                    retrieve=None, order=None, offset=None, limit=None):
        base_key = key
        # Order is significant...
        if key_range is not None:
            if key is not None:
                raise ValueError("Both key and key_range specified")
            base_key = self._key_range_to_key(key_range)
        if query is not None:
            base_key = self._query_to_key(base_key, query)
        if retrieve is not None:
            base_key = self._retrieve_to_key(base_key, retrieve)
        if offset is not None or limit is not None:
            base_key = self._offlim_to_key(base_key, offset, limit)

        # base_key is now properly structured
        self.do_cache_query(items, base_key=base_key)


    def cache_items(self, items):
        self.do_cache_query(items=items)

    def brute_search(self, mc, keyset):
        res = mc.get_multi(keyset)
        ret = []
        for store in res.itervalues():
            store = self.serializer.loads(store)
            if isinstance(store, (list, tuple)):
                # Collection or reference?
                if len(store) == 0:
                    # Empty collection
                    pass
                elif isinstance(store[0], (list, tuple)):
                    # Collection
                    ret.extend(self.brute_search(mc, set([self.separator.join(map(str, item)) for item in store])))
                else:
                    # Reference
                    ret.extend(self.brute_search(mc, set([self.separator.join(map(str, store))])))
            else:
                # Object
                ret.append(store)
        return ret

    def build_invalidation_keyset(self, items, keys, locks, keyset, integrity_keyset, cascade_keyset, force_lock):
        # remove all keys.
        # remove all keys referenced by integrity keys.
        # remove all integrity keys.
        # cascade according to consistency requirements.
        cascade_keydict = {}
        new_integrity_keyset = set()
        for gob in items:
            keyset.add(self.separator.join(self.key_to_mykey(gob.obj_key)))
            for key in itertools.imap(
                    self.key_to_mykey,
                    gob.unique_keyset()):
                keyset.add(self.separator.join(key))
            for key in itertools.imap(
                    self.key_to_mykey,
                    gob.keyset()):
                key = self.separator.join(key)
                keyset.add(key)
                integrity_key = self.integrity_prefix + self.separator + key
                if integrity_key not in integrity_keyset:
                    integrity_keyset.add(integrity_key)
                    new_integrity_keyset.add(integrity_key)
            for consistence in gob.consistency:
                if consistence['invalidate'] == 'cascade':
                    cascade_key = self.separator.join(self.key_to_mykey(consistence['foreign_obj']))
                    if cascade_key not in cascade_keyset:
                        cascade_keyset.add(cascade_key)
                        if consistence['foreign_class'] in cascade_keydict:
                            ks = cascade_keydict[consistence['foreign_class']]
                        else:
                            ks = cascade_keydict[consistence['foreign_class']] = set()
                        ks.add(self.shadow_prefix + self.separator + cascade_key)
        for key in itertools.imap(
                self.key_to_mykey,
                keys):
            key = self.separator.join(key)
            keyset.add(key)
            integrity_key = self.integrity_prefix + self.separator + key
            if integrity_key not in integrity_keyset:
                integrity_keyset.add(integrity_key)
                new_integrity_keyset.add(integrity_key)
        items.clear()
        keys.clear()
        newlocks = set([self.lock_prefix + self.separator + key \
                            for v in cascade_keydict.itervalues() \
                            for key in v] \
                       + [self.lock_prefix + self.separator + key \
                              for key in new_integrity_keyset])
        newlocks -= locks
        if force_lock:
            self.acquire_locks(newlocks)
        else:
            if not self.try_acquire_locks(newlocks):
                raise gobpersist.exception.Deadlock("Could not acquire the locks " + repr(newlocks))
        locks |= newlocks
        with self.pool.reserve(*self.mc_args, **self.mc_kwargs) as mc:
            for cls in cascade_keydict:
                cascade_keydict[cls] = self.brute_search(mc, cascade_keydict[cls])
            integrity_dict = mc.get_multi(new_integrity_keyset)
        for res in integrity_dict.itervalues():
            key_list = self.serializer.loads(res)
            for key in key_list:
                keys.add(tuple(key))
        for cls, goblist in cascade_keydict.iteritems():
            for gob in goblist:
                items.add(self.mygob_to_gob(cls, gob))
        if len(items) != 0 or len(keys) != 0:
            self.build_invalidation_keyset(items, keys, locks, keyset, integrity_keyset, cascade_keyset, force_lock)

    def invalidate(self, items=None, keys=None):
        keyset = set()
        integrity_keyset = set()
        cascade_keyset = set()
        if items is None:
            items = set()
        else:
            items = set(items)
        if keys is None:
            keys = set()
        else:
            keys = set(keys)
        locks = set()
        tries = self.lock_tries
        while(True):
            try:
                self.build_invalidation_keyset(items.copy(), keys.copy(), locks, keyset, integrity_keyset, cascade_keyset, tries == 0)
                keyset |= integrity_keyset | cascade_keyset
                newlocks = set([self.lock_prefix + self.separator + key for key in keyset])
                newlocks -= locks
                if(tries > 0):
                    if not self.try_acquire_locks(newlocks):
                        raise gobpersist.exception.Deadlock("Could not acquire the locks " + repr(newlocks))
                else:
                    self.acquire_locks(newlocks)
                locks |= newlocks
                with self.pool.reserve(*self.mc_args, **self.mc_kwargs) as mc:
                    mc.delete_multi(list(keyset))
            except gobpersist.exception.Deadlock:
                if tries == 0: # How would this happen??
                    raise
                tries -= 1
                keyset.clear()
                integrity_keyset.clear()
                cascade_keyset.clear()
                time.sleep(self.lock_backoff)
                continue
            finally:
                self.release_locks(locks)
                locks.clear()
            break

    def _key_range_to_key(self, key_range):
        return key_range[0] + ('-',) + key_range[1]

    def _serialize_term(self, term):
        if isinstance(term, dict):
            # Quantifier
            if len(term) > 1:
                raise gobpersist.exception.QueryError("Too many keys in quantifier")
            k, v = term.items()[0]
            v = self._serialize_term(v)
            return ('_' + k + '_',) + v
        elif isinstance(term, tuple):
            # Identifier
            return ('(',) + term + (')',)
        else:
            # Literal
            return (repr(term),)

    def _serialize_query(self, query):
        ret = []
        for cmd, args in query.iteritems():
            if len(args) == 0:
                continue
            if cmd in ('gt', 'ge', 'lt', 'le', 'eq', 'ne'):
                if len(args) < 2:
                    continue
                for term in args[:-1]:
                    ret.extend(self._serialize_term(term))
                    ret.append('_' + cmd + '_')
                ret.extend(self._serialize_term(args[-1]))
            elif cmd in ('and', 'or'):
                if len(args) == 1:
                    ret.append(self._serialize_query(args[0]))
                else:
                    for subquery in args[:-1]:
                        ret.append('(')
                        ret.extend(self._serialize_query(subquery))
                        ret.append(')')
                        ret.append('_' + cmd + '_')
                    ret.append('(')
                    ret.extend(self._serialize_query(args[-1]))
                    ret.append(')')
            elif cmd in ('nor', 'not'):
                if len(args) == 1:
                    ret.append('_not_')
                    ret.append('(')
                    ret.extend(self._serialize_query(args[0]))
                    ret.append(')')
                else:
                    for subquery in args[:-1]:
                        ret.append('(')
                        ret.extend(self._serialize_query(subquery))
                        ret.append(')')
                        ret.append('_nor_')
                    ret.append('(')
                    ret.extend(self._serialize_query(args[-1]))
                    ret.append(')')
            else:
                raise gobpersist.exception.QueryError("Unknown query element %s" % repr(cmd))
        return tuple(ret)

    def _query_to_key(self, key, query):
        return key + ('_WHERE_',) + self._serialize_query(query)

    def _retrieve_to_key(self, key, retrieve):
        return key + ('_RETRIEVE_',) + tuple(retrieve)

    def _offlim_to_key(self, key, offset, limit):
        return key + ('_OFFSET_',) \
            + ('_NULL_',) if offset is None else (offset,) \
            + ('_LIMIT_',) \
            + ('_NULL_',) if limit is None else (limit,)
