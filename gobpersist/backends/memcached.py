from __future__ import absolute_import
import time
import cPickle as pickle
import datetime
import itertools

import pylibmc

from . import gobkvquerent
from . import cache
from .. import exception
from .. import session
from .. import field

class PickleWrapper(object):
    loads = pickle.loads

    @staticmethod
    def dumps(obj):
        return pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)


class MemcachedBackend(gobkvquerent.GobKVQuerent):
    """Gob back end which uses memcached for storage"""

    def __init__(self, servers=['127.0.0.1'], expiry=0, binary=True,
                 serializer=PickleWrapper, lock_prefix='_lock',
                 *args, **kwargs):
        behaviors = {'ketama': True, 'cas': True}
        for key, value in kwargs.iteritems():
            behaviors[key] = value

        self.mc = pylibmc.Client(servers, behaviors = behaviors, binary=binary)
        """The memcached client for this back end."""

        self.serializer = serializer
        """The serializer for this back end."""

        self.lock_prefix = lock_prefix
        """A string to prepend to a key value to represent the lock
        for that key."""

        self.expiry = expiry
        """The expiry time for all values set during this session, in
        number of seconds."""

        super(MemcachedBackend, self).__init__()

    def do_kv_multi_query(self, cls, keys):
        keys = [str(".".join(key)) for key in keys]
        res = self.mc.get_multi(keys)
        ret = []
        for key in keys:
            if key not in res:
                raise exception.NotFound(
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
        res = self.mc.get(str(".".join(key)))
        if res == None:
            raise exception.NotFound(
                "Could not find value for key %s" \
                    % ".".join(key))
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
            raise exception.UnsupportedError("key_range is not supported by" \
                                                 " memcached")
        return self.do_kv_query(cls, self.key_to_mykey(key))

    def try_acquire_locks(self, locks):
        """Tries to acquire the locks.
        
        Returns true if successful, false otherwise."""
        locks_acquired = []
        for lock in locks:
            # Lock the object
            if self.mc.add(lock, '1'):
                locks_acquired.append(lock)
            else:
                # The object is locked!
                # Back out all acquired locks
                self.release_locks(locks_acquired)
                return False
        return True

    def acquire_locks(self, locks):
        """Atomically acquires a set of locks."""
        tries = 8
        # After this many tries, we forcibly acquire the locks

        sleep_time = 0.25
        # Time to sleep between tries

        while tries > 0:
            if self.try_acquire_locks(locks):
                return locks
            else:
                # We failed to acquire all the locks; loop and try again
                tries -= 1
                time.sleep(sleep_time)

        # We failed to acquire locks after *tries* attempts.  Say a
        # hail mary and force acquire.
        for lock in locks:
            # Hail Mary!
            # Lock the object
            self.mc.set(lock, 'locked')
        return locks

    def release_locks(self, locks):
        """Releases a set of locks."""
        self.mc.delete_multi(locks)

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
            locks.add(self.lock_prefix + '.' + '.'.join(gob_key))
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
                locks.add(self.lock_prefix + '.' + '.'.join(key))
                to_add.append((key, gob_key))
            for key in itertools.imap(
                    self.key_to_mykey,
                    add_keys):
                locks.add(self.lock_prefix + '.' + '.'.join(key))
                collection_add.append((key, gob_key))

        for update in updates:
            gob = update['gob']
            gob_key = self.key_to_mykey(gob.obj_key)
            locks.add(self.lock_prefix + '.' + '.'.join(gob_key))
            to_set.append((gob_key, self.gob_to_mygob(gob)))
            for key in gob.unique_keyset():
                for f in key:
                    if isinstance(f, field.Field) and f.dirty:
                        new_key = self.key_to_mykey(key)
                        old_key = self.key_to_mykey(key, True)
                        locks.add(self.lock_prefix + '.' + '.'.join(new_key))
                        locks.add(self.lock_prefix + '.' + '.'.join(old_key))
                        to_delete.append(old_key)
                        to_add.append((new_key, gob_key))
                        break
            for key in gob.keyset():
                for f in key:
                    if isinstance(f, field.Field) and f.dirty:
                        new_key = self.key_to_mykey(key)
                        old_key = self.key_to_mykey(key, True)
                        locks.add(self.lock_prefix + '.' + '.'.join(new_key))
                        locks.add(self.lock_prefix + '.' + '.'.join(old_key))
                        collection_remove.append((old_key, gob_key))
                        collection_add.append((new_key, gob_key))
                        break
            if 'add_keys' in update:
                for key in itertools.imap(self.key_to_mykey,
                                           update['add_keys']):
                    locks.add(self.lock_prefix + '.' + '.'.join(key))
                    collection_add.append((key, gob_key))
            if 'add_unique_keys' in update:
                for key in itertools.imap(self.key_to_mykey,
                                           update['add_unique_keys']):
                    locks.add(self.lock_prefix + '.' + '.'.join(key))
                    to_add.append((key, gob_key))
            if 'remove_keys' in update:
                for key in itertools.imap(self.key_to_mykey,
                                           update['remove_keys']):
                    locks.add(self.lock_prefix + '.' + '.'.join(key))
                    collection_remove.append((key, gob_key))
            if 'remove_unique_keys' in update:
                for key in itertools.imap(self.key_to_mykey,
                                           update['remove_unique_keys']):
                    locks.add(self.lock_prefix + '.' + '.'.join(key))
                    to_delete.append(key)

        for removal in removals:
            gob = removal['gob']
            gob_key = self.key_to_mykey(gob.obj_key)
            locks.add(self.lock_prefix + '.' + '.'.join(gob_key))
            to_delete.append(gob_key)
            if 'remove_keys' in removal:
                for key in itertools.imap(self.key_to_mykey,
                                          removal['remove_keys']):
                    locks.add(self.lock_prefix + '.' + '.'.join(key))
                    collection_remove.append((key, gob_key))
            if 'remove_unique_keys' in removal:
                for key in itertools.imap(self.key_to_mykey,
                                          removal['remove_unique_keys']):
                    locks.add(self.lock_prefix + '.' + '.'.join(key))
                    to_delete.append(key)
            for key in itertools.imap(lambda x: self.key_to_mykey(x, True),
                                      gob.keyset()):
                locks.add(self.lock_prefix + '.' + '.'.join(key))
                collection_remove.append((key, gob_key))
            for key in itertools.imap(lambda x: self.key_to_mykey(x, True),
                                      gob.unique_keys):
                locks.add(self.lock_prefix + '.' + '.'.join(key))
                to_delete.append(key)

        for key in itertools.imap(self.key_to_mykey, collection_additions):
            locks.add(self.lock_prefix + '.' + '.'.join(key))
            to_add.append((key, []))

        for key in itertools.imap(self.key_to_mykey, collection_removals):
            locks.add(self.lock_prefix + '.' + '.'.join(key))
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
                            raise exception.Corruption(
                                "Key %s indicates an empty collection" \
                                    " instead of an object." \
                                    % repr(alteration['gob'].obj_key))
                        gob = res[0]
                        if not self._execute_query(gob,
                                                   alteration['conditions']):
                            raise exception.ConditionFailed(
                                "The conditions '%s' could not be met for" \
                                    " object '%s'" \
                                    % (repr(alteration['conditions']),
                                       repr(gob)))
                    except exception.NotFound:
                        # Since this is memcached, we should be
                        # lax about missing values
                        continue

            # Conditions pass! Actually perform the actions
            # print "to_set:", to_set, "to_add:", to_add, \
            #     "to_delete:", to_delete, "collection_add:", collection_add, \
            #     "collection_remove:", collection_remove, \
            #     "locks:", locks, "conditions:", conditions

            add_multi = {}
            for add in to_add:
                add_multi['.'.join(add[0])] = self.serializer.dumps(add[1])
            # no add_multi??
            self.mc.set_multi(add_multi, self.expiry)
            c_addsrms = self.mc.get_multi(['.'.join(c_add[0]) \
                                               for c_add \
                                               in itertools.chain(
                                                   collection_add,
                                                   collection_remove)])
            for key in c_addsrms:
                c_addsrms[key] \
                    = set([tuple(path)
                           for path in self.serializer.loads(c_addsrms[key])])
            for c_add in collection_add:
                key = '.'.join(c_add[0])
                if key in c_addsrms:
                    res = c_addsrms[key]
                else:
                    res = c_addsrms[key] = set()
                res.add(c_add[1])
            for c_rm in collection_remove:
                key = '.'.join(c_add[0])
                if key in c_addsrms:
                    res = c_addsrms[key]
                    res.discard(c_rm[1])
            set_multi = {}
            for k, v in c_addsrms.iteritems():
                set_multi[k] = self.serializer.dumps(list(v))
            for setting in to_set:
                set_multi['.'.join(setting[0])] \
                    = self.serializer.dumps(setting[1])
            self.mc.set_multi(set_multi, self.expiry)
            self.mc.delete_multi(['.'.join(delete) for delete in to_delete])
        finally:
            # Done.  Release the locks.
            self.release_locks(locks)
        # memcached never changes items on update
        return []


class MemcachedCache(MemcachedBackend, cache.Cache):
    """A cache backend based on Memcached."""
    def __init__(self, servers=['127.0.0.1'], expiry=0, binary=True,
                 serializer=PickleWrapper, lock_prefix='_lock',
                 *args, **kwargs):
        MemcachedBackend.__init__(self, servers, expiry, binary,
                                  serializer, lock_prefix,
                                  *args, **kwargs)

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
        except exception.NotFound:
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
            except exception.NotFound:
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
            except exception.NotFound:
                pass
        if offset is not None or limit is not None:
            base_key = self._offlim_to_key(base_key, offset, limit)
            return super(MemcachedCache, self).query(
                cls=cls,
                key=base_key,
                order=order)
        raise exception.NotFound(
            "Could not find value for key %s" \
                % ".".join(self.key_to_mykey(base_key)))


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

        # we add the following entries:
        # 1. Each entry is added as the result of base_key
        # 2. All unique keys for this object are added
        # 3. Base_key is added to (_INTEGRITY_, key) for each key

        base_key = self.key_to_mykey(base_key)
        base_coll = []
        to_set = {}
        integrity_add = []
        locks = set()
        locks.add(self.lock_prefix + '.' + '.'.join(base_key))
        for gob in items:
            gob_key = self.key_to_mykey(gob.obj_key)
            to_set['.'.join(gob_key)] \
                = self.serializer.dumps(self.gob_to_mygob(gob))
            if gob_key == base_key:
                # this was a query on a primary key
                break
            locks.add(self.lock_prefix + '.' + '.'.join(gob_key))
            for key in itertools.imap(
                    self.key_to_mykey,
                    gob.unique_keyset()):
                locks.add(self.lock_prefix + '.' + '.'.join(key))
                to_set['.'.join(key)] = self.serializer.dumps(gob_key)
            for key in itertools.imap(
                    self.key_to_mykey,
                    gob.keyset()):
                key = ('_INTEGRITY_',) + key
                locks.add(self.lock_prefix + '.' + '.'.join(key))
                integrity_add.append((key, base_key))
            base_coll.append(gob_key)
        else:
            # not a query on a primary key
            to_set['.'.join(base_key)] = self.serializer.dumps(base_coll)

        self.acquire_locks(locks)
        try:
            if integrity_add:
                c_adds = self.mc.get_multi(['.'.join(c_add[0]) \
                                                for c_add \
                                                in integrity_add])
                for key in c_adds:
                    c_adds[key] \
                        = set([tuple(path)
                               for path in self.serializer.loads(c_adds[key])])
                for c_add in integrity_add:
                    key = '.'.join(c_add[0])
                    if key in c_adds:
                        res = c_adds[key]
                    else:
                        res = c_adds[key] = set()
                    res.add(c_add[1])
                integrity_set = {}
                for k,v in c_adds.iteritems():
                    integrity_set[k] = self.serializer.dumps(list(v))
                self.mc.set_multi(to_set, self.expiry)
                if self.expiry > 0:
                    self.mc.set_multi(integrity_set, self.expiry + 10)
                else:
                    self.mc.set_multi(integrity_set, self.expiry)
            else:
                self.mc.set_multi(to_set, self.expiry)
        finally:
            self.release_locks(locks)


    def invalidate(self, items=None, keys=None):
        # remove all keys.
        # remove all keys referenced by integrity keys.
        # remove all integrity keys.
        keyset = set()
        integrity_keyset = set()
        if items is not None:
            for gob in items:
                keyset.add('.'.join(self.key_to_mykey(gob.obj_key)))
                for key in itertools.imap(
                        self.key_to_mykey,
                        gob.unique_keyset()):
                    keyset.add('.'.join(key))
                for key in itertools.imap(
                        self.key_to_mykey,
                        gob.keyset()):
                    keyset.add('.'.join(key))
                    integrity_keyset.add('.'.join(('_INTEGRITY_',) + key))
        if keys is not None:
            for key in itertools.imap(
                    self.key_to_mykey,
                    keys):
                keyset.add('.'.join(key))
                integrity_keyset.add('.'.join(('_INTEGRITY_',) + key))

        tries = 8
        sleep_time = 0.25

        first_locks = set([self.lock_prefix + '.' + key \
                               for key in itertools.chain(keyset,
                                                          integrity_keyset)])
        while tries > 0:
            self.acquire_locks(first_locks)
            try:
                integrity_dict = self.mc.get_multi(integrity_keyset)
                keyset_extra = set()
                for v in integrity_dict.itervalues():
                    key_list = self.serializer.loads(v)
                    for key in key_list:
                        keyset_extra.add(str('.'.join(key)))
                second_locks = set([self.lock_prefix + '.' + key \
                                        for key in keyset_extra]) \
                               - first_locks
                if not self.try_acquire_locks(second_locks):
                    self.release_locks(first_locks)
                    tries -= 1
                    time.sleep(sleep_time)
                    continue
                else:
                    # successfully acquired all locks
                    break
            except:
                self.release_locks(first_locks)
                raise
        else:
            # we ran out of tries; say a hail mary and force the
            # acquisition
            self.acquire_locks(first_locks)
            try:
                integrity_dict = self.mc.get_multi(integrity_keyset)
                keyset_extra = set()
                for v in integrity_dict.itervalues():
                    key_list = self.serializer.loads(v)
                    for key in key_list:
                        keyset_extra.add(str('.'.join(key)))
                second_locks = set([self.lock_prefix + '.' + key \
                                        for key in keyset_extra]) \
                               - first_locks
                self.acquire_locks(second_locks)
            except:
                self.release_locks(first_locks)
                raise
        try:
            self.mc.delete_multi(list(keyset | keyset_extra | integrity_keyset))
        finally:
            self.release_locks(first_locks | second_locks)

    def _key_range_to_key(self, key_range):
        return key_range[0] + ('-',) + key_range[1]

    def _serialize_term(self, term):
        if isinstance(term, dict):
            # Quantifier
            if len(term) > 1:
                raise exception.QueryError("Too many keys in quantifier")
            k, v = term.items()[0]
            v = _serialize_term(v)
            return ['_' + k + '_'] + v
        else:
            # Identifier
            return ['('] + term + [')']

    def _serialize_query(self, query):
        ret = []
        for cmd, args in query:
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
                raise exception.QueryError("Unknown query element %s" % repr(cmd))
        return tuple(ret)

    def _query_to_key(self, key, query):
        return key + ('_WHERE_',) + self._serialize_query(query)

    def _retrieve_to_key(self, key, retrieve):
        return key + ('_RETRIEVE_',) + tuple(retrieve)

    def _offlim_to_key(key, offset, limit):
        return key + ('_OFFSET_',) \
            + ('_NULL_',) if offset is None else (offset,) \
            + ('_LIMIT_',) \
            + ('_NULL_',) if limit is None else (limit,)
