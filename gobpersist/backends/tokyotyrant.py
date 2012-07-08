# tokyotyrant.py - Back end interface to Tokyo Tyrant
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
"""Back end interface to Tokyo Tyrant.

.. codeauthor:: Evan Buswell <evan.buswell@accellion.com
"""

import time
import cPickle as pickle
import datetime
import itertools
import socket

import pytyrant

import gobpersist.backends.gobkvquerent
import gobpersist.exception
import gobpersist.field
import gobpersist.backends.pools

# These ought to be defined in pytyrant
PYTTINVALID = 1
PYTTNOHOST = 2
PYTTREFUSED = 3
PYTTSEND = 4
PYTTRECV = 5
PYTTKEEP = 6
PYTTNOREC = 7
PYTTMISC = 9999

class PickleWrapper(object):
    loads = pickle.loads

    @staticmethod
    def dumps(obj):
        return pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)


class TyrantClient(pytyrant.Tyrant):
    """Wrapper class to create a "Client" class suitable for use with
    the :class:`gobpersist.backends.pools.SimpleThreadMappedPool`.
    """
    def __init__(self, host='127.0.0.1', port=pytyrant.DEFAULT_PORT,
                 unix=None):
        """
        Args:
           ``host``: The hostname to connect to.

           ``port``: The port to connect to.

           ``unix``: Alternately, the path of a unix socket to connect
           to.
        """
        if unix is not None:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(unix)
        else:
            sock = socket.socket()
            sock.connect((host, port))
            sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        super(TyrantClient, self).__init__(sock)

default_pool = gobpersist.backends.pools.SimpleThreadMappedPool(client=TyrantClient)

class TokyoTyrantBackend(gobpersist.backends.gobkvquerent.GobKVQuerent):
    """Gob back end which uses Tokyo Tyrant for storage"""

    def __init__(self, host='127.0.0.1', port=pytyrant.DEFAULT_PORT,
                 unix=None, serializer=PickleWrapper, lock_prefix='_lock',
                 pool=default_pool, separator='.', lock_tries=8,
                 lock_backoff=0.25):
        """
        Args:
           ``host``: The hostname to connect to.

           ``port``: The port to connect to.

           ``unix``: Alternately, the path of a unix socket to connect
           to.

           ``serializer``: An object which provides serialization of
           gobpersist data.

              Must provide ``loads`` and ``dumps``.

           ``lock_prefix``: A string to prepend to a key value to
           represent the lock for that key.

           ``pool``: They pool of tokyo tyrant connections.

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
        self.tt_args = ()
        self.tt_kwargs = {'host': host, 'port': port, 'unix': unix}
        self.pool = pool

        self.serializer = serializer
        """An object which provides serialization of gobpersist data.

        Must provide ``loads`` and ``dumps``."""

        self.lock_prefix = lock_prefix
        """A string to prepend to a key value to represent the lock
        for that key."""

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

        super(TokyoTyrantBackend, self).__init__()

    def do_kv_multi_query(self, cls, keys):
        keys = [str(self.separator.join(key)) for key in keys]
        with self.pool.reserve(*self.tt_args, **self.tt_kwargs) as tyrant:
            try:
                res = tyrant.mget(keys)
            except pytyrant.TyrantError as terr:
                if terr.args[0] == PYTTNOREC:
                    raise gobpersist.exception.NotFound(
                        "Could not find value for key %s" \
                            % self.separator.join(key))
                else:
                    raise
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
            raise gobpersist.exception.NotFound(
                "Could not find value for key %s" \
                    % keys.pop())
        return ret

    def do_kv_query(self, cls, key):
        with self.pool.reserve(*self.tt_args, **self.tt_kwargs) as tyrant:
            try:
                res = tyrant.get(str(self.separator.join(key)))
            except pytyrant.TyrantError as terr:
                if terr.args[0] == PYTTNOREC:
                    raise gobpersist.exception.NotFound(
                        "Could not find value for key %s" \
                            % self.separator.join(key))
                else:
                    raise
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
            raise gobpersist.exception.UnsupportedError("key_range is not yet supported by" \
                                                            " TokyoTyrantBackend")
        return self.do_kv_query(cls, self.key_to_mykey(key))

    def acquire_locks(self, locks):
        """Atomically acquires a set of locks.

        Be very careful calling this function from outside this
        module, as the locking mechanism was does not support holding
        locks for long periods of time.
        """
        tries = self.lock_tries
        # After this many tries, we forcibly acquire the locks

        locks_acquired = []
        with self.pool.reserve(*self.tt_args, **self.tt_kwargs) as tyrant:
            while tries > 0:
                try:
                    for lock in locks:
                        # Lock the object
                        tyrant.putkeep(lock, '1')
                        locks_acquired.append(lock)
                except pytyrant.TyrantError as terr:
                    # Back out all acquired locks
                    self.release_locks(locks_acquired)
                    if terr.args[0] == PYTTKEEP:
                        # The object is locked!  Start over
                        locks_acquired = []
                        tries -= 1
                        time.sleep(self.lock_backoff)
                    else:
                        raise
                except:
                    # Back out all acquired locks
                    self.release_locks(locks_acquired)
                    raise
                else:
                    # We acquired all the locks
                    return locks_acquired

            # We failed to acquire locks after *tries* attempts.  Say a
            # hail mary and force acquire.
            tyrant.misc("putlist", 0, [item for lock in locks for item in (lock, '1')])

    def release_locks(self, locks):
        """Releases a set of locks."""
        with self.pool.reserve(*self.tt_args, **self.tt_kwargs) as tyrant:
            tyrant.misc("outlist", 0, locks)

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
                        raise gobpersist.exception.ConditionFailed(
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
                add_multi.append((self.separator.join(add[0]), self.serializer.dumps(add[1])))
            # no putkeeplist??
            with self.pool.reserve(*self.tt_args, **self.tt_kwargs) as tyrant:
                tyrant.misc("putlist", 0, [item for tuple_ in add_multi for item in tuple_])
                c_addsrms_list = tyrant.mget([self.separator.join(c_add[0]) \
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
                set_multi = []
                for k, v in c_addsrms.iteritems():
                    set_multi.append((k, self.serializer.dumps(list(v))))
                for setting in to_set:
                    set_multi.append((self.separator.join(setting[0]),
                                      self.serializer.dumps(setting[1])))
                tyrant.misc("putlist", 0, [item for tuple_ in set_multi for item in tuple_])
                tyrant.misc("outlist", 0, [self.separator.join(delete) for delete in to_delete])
        finally:
            # Done.  Release the locks.
            self.release_locks(locks)
        # this never changes items on update
        return []
