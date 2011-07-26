from __future__ import absolute_import
from . import gobkvquerent
from .. import exception

import pylibmc
import time
import cPickle as pickle
import datetime

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


    def do_query(self, path, query=None, retrieve=None, offset=None, limit=None):
        # ignore everything but path
        res = self.mc.get(".".join(path))
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
                    ret.append(self.do_query(path)[0])
                    return ret
            else:
                # Reference
                return [self.do_query(path=store)]
        else:
            # Object
            return [store]


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
                while True:
                    (status, cas) = self.mc.gets(lock)
                    print "status=%s cas=%s" % (repr(status), repr(cas))
                    if status == "\0":
                        # Lock the object
                        if self.mc.cas(lock, "\1", cas):
                            locks_acquired.append(lock)
                            break
                        else:
                            continue
                    elif status is None:
                        # Lock the object
                        if self.mc.add(lock, "\1"):
                            locks_acquired.append(lock)
                            break
                        else:
                            continue                        
                    else:
                        # The object is locked!
                        # Back out all acquired locks and start over
                        self.release_locks(locks_acquired)
                        locks_acquired = []
                        break
                # Did we break due to success, or due to failure?
                if not locks_acquired:
                    break
                else:
                    continue

            # Did we break to retry, or did we finish?
            if locks_acquired:
                return locks_acquired
            else:
                # We failed to acquire all the locks; loop and try again
                tries -= 1
                time.sleep(sleep_time)
                continue
        # We failed to acquire locks after *tries* attempts.  Say a
        # hail mary and force acquire.
        for lock in locks:
            # Hail Mary!
            # Lock the object
            self.mc.set(lock, "\1")
            locks_acquired.append(lock)


    def release_locks(self, locks):
        """Releases a set of locks."""
        for lock in locks:
            self.mc.set(lock, "\0")

    def value_to_pvalue(self, value, use_persisted_version=False):
        value = self.caller._value_to_pvalue(value, use_persisted_version)
        if isinstance(value, datetime.datetime):
            return value.isoformat()
        elif value is None:
            return '_NULL'
        else:
            return value

    def do_commit(self, operations):
        # Build the set of commits
        to_set = []
        to_add = []
        to_delete = []
        collection_add = []
        collection_remove = []
        locks = []
        conditions = []

        print repr(operations)

        for addition in operations['additions']:
            to_add.append((addition['path'], addition['item'],))
            locks.append(self.lock_prefix + '.' + '.'.join(addition['path']))
            for path in addition['unique_keys']:
                to_add.append((path, addition['path'],))
                locks.append(self.lock_prefix + '.' + '.'.join(path))
            for path in addition['keys']:
                collection_add.append((path, addition['path'],))
                locks.append(self.lock_prefix + '.' + '.'.join(path))

        for update in operations['updates']:
            to_set.append((update['path'], update['item'],))
            locks.append(self.lock_prefix + '.' + '.'.join(update['path']))
            for path in update['old_unique_keys']:
                to_delete.append(path)
                locks.append(self.lock_prefix + '.' + '.'.join(path))
            for path in update['old_keys']:
                collection_remove.append((path, update['path'],))
                locks.append(self.lock_prefix + '.' + '.'.join(path))
            for path in update['new_unique_keys']:
                to_add.append((path, update['path'],))
                locks.append(self.lock_prefix + '.' + '.'.join(path))
            for path in update['new_keys']:
                collection_add.append((path, update['path'],))
                locks.append(self.lock_prefix + '.' + '.'.join(path))

        for removal in operations['removals']:
            to_delete.append(removal['path'])
            locks.append(self.lock_prefix + '.' + '.'.join(removal['path']))
            for path in removal['unique_keys']:
                to_delete.append(path)
                locks.append(self.lock_prefix + '.' + '.'.join(path))
            for path in removal['keys']:
                collection_remove.append((path, removal['path'],))
                locks.append(self.lock_prefix + '.' + '.'.join(path))

        # Acquire locks
        self.acquire_locks(locks)
        try:
            
            # Check all conditions
            for update in operations['updates']:
                if 'conditions' in update:
                    try:
                        # DON'T replace this with a simple .query()
                        # call, as that will interact with
                        # deduplication in weird ways.
                        gob = self.caller.create_gob(self.do_query(update['path'])[0])
                        if not self._execute_query(gob, update['conditions']):
                            raise ConditionFailed(
                                "The conditions '%s' could not be met for" \
                                    " object '%s'" % (repr(update['conditions']),
                                                      repr(update['path'])))
                    except exception.NotFound:
                        continue

            for removal in operations['removals']:
                if 'conditions' in removal:
                    try:
                        # DON'T replace this with a simple .query()
                        # call, as that will interact with
                        # deduplication in weird ways.
                        gob = self.caller.create_gob(self.do_query(removal['path'])[0])
                        if not self._execute_query(gob, removal['conditions']):
                            raise ConditionFailed(
                                "The conditions '%s' could not be met for" \
                                    " object '%s'" % (repr(removal['conditions']),
                                                      repr(removal['path'])))
                    except exception.NotFound:
                        continue

            # Conditions pass! Actually perform the actions
            print "to_set:", to_set, "to_add:", to_add, "to_delete:", to_delete, "collection_add:", collection_add, "collection_remove:", collection_remove, "locks:", locks, "conditions:", conditions


            for add in to_add:
                self.mc.add('.'.join(add[0]), self.serializer.dumps(add[1]))
            for c_add in collection_add:
                res = self.mc.get('.'.join(c_add[0]))
                if res is None:
                    self.mc.set('.'.join(c_add[0]), self.serializer.dumps([c_add[1]]))
                else:
                    res = set(self.serializer.loads(res))
                    res.add(c_add[1])
                    self.mc.set('.'.join(c_add[0]), self.serializer.dumps(list(res)))
            for setting in to_set:
                self.mc.set('.'.join(setting[0]), self.serializer.dumps(setting[1]))
            for delete in to_delete:
                self.mc.delete('.'.join(delete))
            for c_rm in collection_remove:
                res = self.mc.get('.'.join(c_rm[0]))
                if res is not None:
                    res = set(self.serializer.loads(res))
                    res.discard(c_rm[1])
                    self.mc.set('.'.join(c_rm[0]), self.serializer.dumps(list(res)))
        finally:
            # Done.  Release the locks.
            self.release_locks(locks)
        # memcached never changes items on update
        return {}
