from __future__ import absolute_import
import time
import cPickle as pickle
import datetime
import itertools
import socket

import pytyrant

from . import gobkvquerent
from .. import exception
from .. import session
from .. import field

class PickleWrapper(object):
    loads = pickle.loads

    @staticmethod
    def dumps(obj):
        return pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)


class TokyoTyrantBackend(gobkvquerent.GobKVQuerent):
    """Gob back end which uses Tokyo Tyrant for storage"""

    def __init__(self, host='127.0.0.1', port=pytyrant.DEFAULT_PORT,
                 unix=None, serializer=PickleWrapper, lock_prefix='_lock'):
        self.tyrant = None
        """The tokyo tyrant client for this back end."""
        if unix is None:
            self.tyrant = pytyrant.Tyrant.open(host, port)
        else:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(unix)
            self.tyrant = pytyrant.Tyrant(sock)

        self.serializer = serializer
        """The serializer for this back end."""

        self.lock_prefix = lock_prefix
        """A string to prepend to a key value to represent the lock
        for that key."""

        super(TokyoTyrantBackend, self).__init__()

    def do_kv_multi_query(self, cls, keys):
        keys = [str(".".join(key)) for key in keys]
        try:
            res = self.tyrant.mget(keys)
        except TyrantError:
            raise exception.NotFound(
                "Could not find value for key %s" \
                    % ".".join(key))
        keys = set(keys)
        ret = []
        for key, value in res:
            keys.discard(key)
            store = self.serializer.loads(value)
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
        if len(keys) > 0:
            raise exception.NotFound(
                "Could not find value for key %s" \
                    % keys.pop())
        return ret

    def do_kv_query(self, cls, key):
        try:
            res = self.tyrant.get(str(".".join(key)))
        except TyrantError:
            raise exception.NotFound(
                "Could not find value for key %s" \
                    % ".".join(key))
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
            raise exception.UnsupportedError("key_range is not yet supported by" \
                                                 " TokyoTyrantBackend")
        return self.do_kv_query(cls, self.key_to_mykey(key))

    def acquire_locks(self, locks):
        """Atomically acquires a set of locks."""
        # Can I just say how much simpler and clearer this method
        # would be with a goto command?  The ideological prejudice
        # against this sort of thing has really gone far enough.

        tries = 8
        # After this many tries, we forcibly acquire the locks

        sleep_time = 0.25
        # Time to sleep between tries

        locks_acquired = []
        while tries > 0:
            try:
                for lock in locks:
                    # Lock the object
                    self.tyrant.putkeep(lock, '1')
                    locks_acquired.append(lock)
            except pytyrant.TyrantError:
                # The object is locked!
                # Back out all acquired locks and start over
                self.release_locks(locks_acquired)
                locks_acquired = []
                tries -= 1
                time.sleep(sleep_time)
            else:
                # We acquired all the locks
                return locks_acquired

        # We failed to acquire locks after *tries* attempts.  Say a
        # hail mary and force acquire.
        self.tyrant.misc("putlist", 0, [item for lock in locks for item in (lock, '1')])

    def release_locks(self, locks):
        """Releases a set of locks."""
        self.tyrant.misc("outlist", 0, locks)

    def key_to_mykey(self, key, use_persisted_version=False):
        mykey = super(TokyoTyrantBackend, self).key_to_mykey(key,
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
                        raise exception.ConditionFailed(
                            "The conditions '%s' could not be met for" \
                                " object '%s', as the object could not be found" \
                                % (repr(alteration['conditions']),
                                   repr(alteration['gob'].obj_key)))

            # Conditions pass! Actually perform the actions
            # print "to_set:", to_set, "to_add:", to_add, \
            #     "to_delete:", to_delete, "collection_add:", collection_add, \
            #     "collection_remove:", collection_remove, \
            #     "locks:", locks, "conditions:", conditions

            add_multi = []
            for add in to_add:
                add_multi.append(('.'.join(add[0]), self.serializer.dumps(add[1])))
            # no add_multi??
            self.tyrant.misc("putlist", 0, [item for tuple_ in add_multi for item in tuple_])
            c_addsrms_list = self.tyrant.mget(['.'.join(c_add[0]) \
                                                  for c_add \
                                                  in itertools.chain(
                                                      collection_add,
                                                      collection_remove)])
            c_addsrms = {}
            for key, value in c_addsrms_list:
                c_addsrms[key] \
                    = set([tuple(path)
                           for path in self.serializer.loads(value)])
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
            set_multi = []
            for k, v in c_addsrms.iteritems():
                set_multi.append((k, self.serializer.dumps(list(v))))
            for setting in to_set:
                set_multi.append(('.'.join(setting[0]),
                                  self.serializer.dumps(setting[1])))
            self.tyrant.misc("putlist", 0, [item for tuple_ in set_multi for item in tuple_])
            self.tyrant.misc("outlist", 0, ['.'.join(delete) for delete in to_delete])
        finally:
            # Done.  Release the locks.
            self.release_locks(locks)
        # this never changes items on update
        return []