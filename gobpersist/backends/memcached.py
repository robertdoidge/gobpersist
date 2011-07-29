from __future__ import absolute_import
import time
import cPickle as pickle
import datetime
import itertools

import pylibmc

from . import gobkvquerent
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

    def __init__(self, servers=['127.0.0.1'], binary=True,
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

        super(MemcachedBackend, self).__init__()


    def do_kv_query(self, path, cls):
        res = self.mc.get(str(".".join(path)))
        if res == None:
            raise exception.NotFound(
                "Could not find value for key %s" \
                    % ".".join(path))
        store = self.serializer.loads(res)
        if isinstance(store, (list, tuple)):
            # Collection or reference?
            if len(store) == 0:
                # Empty collection
                return store
            elif isinstance(store[0], (list, tuple)):
                # Collection
                ret = []
                for path in store:
                    ret.append(self.do_kv_query(path, cls)[0])
                return ret
            else:
                # Reference
                return self.do_kv_query(store, cls)
        else:
            # Object
            return [self.mygob_to_gob(cls, store)]


    def kv_query(self, path=None, path_range=None):
        if path_range is not None:
            raise exception.UnsupportedError("path_range is not supported by" \
                                                 " memcached")
        return self.do_kv_query(self.path_to_mypath(path),
                                self.cls_for_path(path))


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
            for lock in locks:
                # Lock the object
                if self.mc.add(lock, 'locked'):
                    locks_acquired.append(lock)
                else:
                    # The object is locked!
                    # Back out all acquired locks and start over
                    self.release_locks(locks_acquired)
                    locks_acquired = []
                    break
            else:
                # We acquired all the locks
                return locks_acquired

            # We failed to acquire all the locks; loop and try again
            tries -= 1
            time.sleep(sleep_time)

        # We failed to acquire locks after *tries* attempts.  Say a
        # hail mary and force acquire.
        for lock in locks:
            # Hail Mary!
            # Lock the object
            self.mc.set(lock, 'locked')
            locks_acquired.append(lock)


    def release_locks(self, locks):
        """Releases a set of locks."""
        for lock in locks:
            self.mc.delete(lock)


    def path_to_mypath(self, path, use_persisted_version=False):
        mypath = super(MemcachedBackend, self).path_to_mypath(
            path,
            use_persisted_version)
        return tuple([pathelem.isoformat() \
                              if isinstance(pathelem, datetime.datetime) \
                          else '_NULL_' if pathelem is None \
                          else str(pathelem) \
                          for pathelem in mypath])


    def do_add_collection(self, path):
        self.acquire_locks((self.lock_prefix + '.' + '.'.join(path),))
        try:
            self.mc.set('.'.join(path), self.serializer.dumps([]))
        finally:
            self.release_locks((self.lock_prefix + '.' + '.'.join(path),))


    def do_remove_collection(self, path):
        self.acquire_locks((self.lock_prefix + '.' + '.'.join(path),))
        try:
            self.mc.delete('.'.join(path))
        finally:
            self.release_locks((self.lock_prefix + '.' + '.'.join(path),))


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
            gob_path = self.path_to_mypath(gob.path())
            locks.add(self.lock_prefix + '.' + '.'.join(gob_path))
            to_add.append((gob_path,
                           self.gob_to_mygob(gob)))
            if gob.store_in_root():
                collection_add.append(
                    (self.path_to_mypath(gob.coll_path), gob_path))
            if 'add_unique_keys' in addition:
                add_unique_keys = itertools.chain(
                    gob.unique_keys,
                    addition['add_unique_keys'])
            else:
                add_unique_keys = gob.unique_keys
            if 'add_keys' in addition:
                add_keys = itertools.chain(
                    gob.keys,
                    addition['add_keys'])
            else:
                add_keys = gob.keys
            for path in itertools.imap(
                self.path_to_mypath,
                add_unique_keys):
                locks.add(self.lock_prefix + '.' + '.'.join(path))
                to_add.append((path, gob_path))
            for path in itertools.imap(
                self.path_to_mypath,
                add_keys):
                    locks.add(self.lock_prefix + '.' + '.'.join(path))
                    collection_add.append((path, gob_path))


        for update in updates:
            gob = update['gob']
            gob_path = self.path_to_mypath(gob.path())
            locks.add(self.lock_prefix + '.' + '.'.join(gob_path))
            to_set.append((gob_path,
                           self.gob_to_mygob(gob)))
            if gob.store_in_root_changed():
                if gob.store_in_root():
                    collection_add.append(
                        (self.path_to_mypath(gob.coll_path), gob_path))
                else:
                    collection_remove.append(
                        self.path_to_mypath(gob.coll_path))
            for path in gob.unique_keys:
                for f in path:
                    if isinstance(f, field.Field) and f.dirty:
                        new_path = self.path_to_mypath(path)
                        old_path = self.path_to_mypath(path, True)
                        locks.add(self.lock_prefix + '.' + '.'.join(new_path))
                        locks.add(self.lock_prefix + '.' + '.'.join(old_path))
                        to_delete.append(old_path)
                        to_add.append((new_path, gob_path))
                        break
            for path in gob.keys:
                for f in path:
                    if isinstance(f, field.Field) and f.dirty:
                        new_path = self.path_to_mypath(path)
                        old_path = self.path_to_mypath(path, True)
                        locks.add(self.lock_prefix + '.' + '.'.join(new_path))
                        locks.add(self.lock_prefix + '.' + '.'.join(old_path))
                        collection_remove.append((old_path, gob_path))
                        collection_add.append((new_path, gob_path))
                        break
            if 'add_keys' in update:
                for path in itertools.imap(self.path_to_mypath,
                                           update['add_keys']):
                    locks.add(self.lock_prefix + '.' + '.'.join(path))
                    collection_add.append((path, gob_path))
            if 'add_unique_keys' in update:
                for path in itertools.imap(self.path_to_mypath,
                                           update['add_unique_keys']):
                    locks.add(self.lock_prefix + '.' + '.'.join(path))
                    to_add.append((path, gob_path))
            if 'remove_keys' in update:
                for path in itertools.imap(self.path_to_mypath,
                                           update['remove_keys']):
                    locks.add(self.lock_prefix + '.' + '.'.join(path))
                    collection_remove.append((path, gob_path))
            if 'remove_unique_keys' in update:
                for path in itertools.imap(self.path_to_mypath,
                                           update['remove_unique_keys']):
                    locks.add(self.lock_prefix + '.' + '.'.join(path))
                    to_delete.append(path)

        for removal in removals:
            gob = removal['gob']
            gob_path = self.path_to_mypath(gob.path())
            locks.add(self.lock_prefix + '.' + '.'.join(gob_path))
            to_delete.append(gob_path)
            if (gob.store_in_root_changed() and not gob.store_in_root()) \
                    or gob.store_in_root():
                collection_remove.append(
                    self.path_to_mypath(gob.coll_path))
            if 'remove_keys' in removal:
                for path in itertools.imap(self.path_to_mypath,
                                           removal['remove_keys']):
                    locks.add(self.lock_prefix + '.' + '.'.join(path))
                    collection_remove.append((path, gob_path))
            if 'remove_unique_keys':
                for path in itertools.imap(self.path_to_mypath,
                                           removal['remove_unique_keys']):
                    locks.add(self.lock_prefix + '.' + '.'.join(path))
                    to_delete.append(path)
            for path in itertools.imap(lambda x: self.path_to_mypath(x, True),
                                       gob.keys):
                locks.add(self.lock_prefix + '.' + '.'.join(path))
                collection_remove.append((path, gob_path))
            for path in itertools.imap(lambda x: self.path_to_mypath(x, True),
                                       gob.unique_keys):
                locks.add(self.lock_prefix + '.' + '.'.join(path))
                to_delete.append(path)

        for path in itertools.imap(self.path_to_mypath, collection_additions):
            locks.add(self.lock_prefix + '.' + '.'.join(path))
            to_add.append((path, []))

        for path in itertools.imap(self.path_to_mypath, collection_removals):
            locks.add(self.lock_prefix + '.' + '.'.join(path))
            to_delete.append(path)

        # Acquire locks
        self.acquire_locks(locks)
        try:
            
            # Check all conditions
            for alteration in itertools.chain(updates, removals):
                if 'conditions' in alteration:
                    try:
                        res = self.kv_query(alteration['gob'].path())
                        if len(res) == 0:
                            # A collection instead of an object??
                            # This indicates some kind of corruption...
                            raise exception.Corruption(
                                "Path %s indicates an empty collection" \
                                    " instead of an object." \
                                    % repr(alteration['gob'].path()))
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
            print "to_set:", to_set, "to_add:", to_add, \
                "to_delete:", to_delete, "collection_add:", collection_add, \
                "collection_remove:", collection_remove, \
                "locks:", locks, "conditions:", conditions


            for add in to_add:
                self.mc.add('.'.join(add[0]), self.serializer.dumps(add[1]))
            for c_add in collection_add:
                res = self.mc.get('.'.join(c_add[0]))
                if res is None:
                    self.mc.set('.'.join(c_add[0]),
                                self.serializer.dumps([c_add[1]]))
                else:
                    res = set([tuple(path) \
                                   for path in self.serializer.loads(res)])
                    res.add(c_add[1])
                    self.mc.set('.'.join(c_add[0]),
                                self.serializer.dumps(list(res)))
            for setting in to_set:
                self.mc.set('.'.join(setting[0]),
                            self.serializer.dumps(setting[1]))
            for c_rm in collection_remove:
                res = self.mc.get('.'.join(c_rm[0]))
                if res is not None:
                    res = set([tuple(path) \
                                   for path in self.serializer.loads(res)])
                    res.discard(c_rm[1])
                    self.mc.set('.'.join(c_rm[0]),
                                self.serializer.dumps(list(res)))
            for delete in to_delete:
                self.mc.delete('.'.join(delete))
        finally:
            # Done.  Release the locks.
            self.release_locks(locks)
        # memcached never changes items on update
        return []
