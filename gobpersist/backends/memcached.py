from __future__ import absolute_import
from . import gobkvquerent

import pylibmc
import time
import cpickle

class PickleWrapper(object):
    loads = cpickle.loads

    @staticmethod
    def dumps(obj):
        return cpickle.dumps(obj, cpickle.HIGHEST_PROTOCOL)


class MemcachedBackend(GobKVQuerent):
    """Gob back end which uses memcached for storage"""

    def __init__(servers=['127.0.0.1'], binary=True, serializer=PickleWrapper,
                 lock_prefix='_lock', *args, **kwargs):

        self.mc = pylibmc.Client(servers, binary)
        """The memcached client for this back end."""

        self.serializer = serializer
        """The serializer for this back end."""

        self.mc.behaviors['ketama'] = True
        self.mc.behaviors['cas'] = True
        for key, value in kwargs.iteritems:
            self.mc.behaviors[key] = value

        super(MemcachedBackend, self).__init__()


    def do_query(self, path, query=None, retrieve=None, offset=None, limit=None):
        # ignore everything but path
        res = self.mc.get("\0".join(path))
        if res == None:
            raise exception.NotFound(
                "Could not find value for key %s" \
                    % "\0".join(path))
        store = self.serializer.loads(res)
        if isinstance(store, list):
            # Collection or reference?
            if len(store) == 0:
                # Empty collection
                return store
            elif isinstance(store[0], list):
                # Collection
                ret = []
                for path in store:
                    ret.append(self.do_query(path))
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
                    if status is None or status == "\0":
                        # Lock the object
                        if self.mc.cas(lock, "\1", cas):
                            locks_acquired.append(path)
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
                tries--
                time.sleep(sleep_time)
                continue
        # We failed to acquire locks after *tries* attempts.  Say a
        # hail mary and force acquire.
        for lock in locks:
            # Hail Mary!
            # Lock the object
            self.mc.set(lock, "\1")
            locks_acquired.append(path)


    def release_locks(self, locks):
        """Releases a set of locks."""
        for lock in locks:
            self.mc.set(lock, "\0")


    def do_commit(self, operations):
        # Build the set of commits
        to_set = []
        to_add = []
        to_delete = []
        collection_add = []
        collection_remove = []
        locks = []
        conditions = []

        for addition in operations['additions']:
            to_add.append((addition['path'], addition['item'],))
            locks.append(self.lock_prefix + '\0' + '\0'.join(addition['path']))
            for path in addition['unique_keys']:
                to_add.append((path, addition['path'],))
                locks.append(self.lock_prefix + '\0' + '\0'.join(addition['path']))
            for path in addition['keys']:
                collection_add.append((path, addition['path'],))
                locks.append(self.lock_prefix + '\0' + '\0'.join(addition['path']))

        for update in operations['updates']:
            to_set.append((update['path'], update['item'],))
            locks.append(self.lock_prefix + '\0' + '\0'.join(update['path']))
            for path in update['old_unique_keys']:
                to_delete.append(path)
                locks.append(self.lock_prefix + '\0' + '\0'.join(update['path']))
            for path in update['old_keys']:
                collection_remove.append((path, update['path'],))
                locks.append(self.lock_prefix + '\0' + '\0'.join(update['path']))
            for path in update['new_unique_keys']:
                to_add.append((path, update['path'],))
                locks.append(self.lock_prefix + '\0' + '\0'.join(update['path']))
            for path in update['new_keys']:
                collection_add.append((path, update['path'],))
                locks.append(self.lock_prefix + '\0' + '\0'.join(update['path']))

        for removal in operations['removals']:
            to_delete.append(removal['path'])
            locks.append(self.lock_prefix + '\0' + '\0'.join(removal['path']))
            for path in removal['unique_keys']:
                to_delete.append(path)
                locks.append(self.lock_prefix + '\0' + '\0'.join(removal['path']))
            for path in removal['keys']:
                collection_remove.append((path, removal['path'],))
                locks.append(self.lock_prefix + '\0' + '\0'.join(removal['path']))

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
            for add in to_add:
                self.mc.add('\0'.join(add[0]), self.serializer.dumps(add[1]))
            for c_add in collection_add:
                res = self.mc.get('\0'.join(c_add[0]))
                if res is None:
                    self.mc.set('\0'.join(c_add[0]), serializer.dumps([c_add[1]]))
                else:
                    res = set(serializer.loads(res))
                    res.add(c_add[1])
                    self.mc.set('\0'.join(c_add[0]), serializer.dumps(res))
            for set in to_set:
                self.mc.set('\0'.join(set[0]), self.serializer.dumps(set[1]))
            for delete in to_delete:
                self.mc.delete('\0'.join(delete))
            for c_rm in collection_remove:
                res = self.mc.get('\0'.join(c_rm[0]))
                if res is not None:
                    res = set(serializer.loads(res))
                    res.discard(c_[1])
                    self.mc.set('\0'.join(c_rm[0]), serializer.dumps(res))
        finally:
            # Done.  Release the locks.
            self.release_locks(locks)
