# gobkvquerent.py - Abstract superclass for key-value stores
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
"""Support for classes that need to explicitly store keys (that is,
key--value stores).

.. codeauthor:: Evan Buswell <evan.buswell@accellion.com>
"""
import itertools

import gobpersist.field
import gobpersist.exception
import gobpersist.backends.gobkvquerent

class GobKVBackend(gobpersist.backends.gobkvquerent.GobKVQuerent):
    def kv_commit(self, add_gobs={}, update_gobs={}, remove_gobs={}, add_keys=set(), remove_keys=set(),
                  add_unique_keys={}, update_unique_keys={}, remove_unique_keys={},
                  collection_additions=set(), collection_removals=set(), conditions={},
                  affected_keys=set()):
        """Commit, tailored for key--value stores.

        Subclasses should override this method.

        Args:
           `add_gobs`: a dictionary mapping from primary key to the
           gob to be added under that key.

           `update_gobs`: a dictionary mapping from primary key to a
           tuple containing the gob to be removed from that key
           followed by the gob to be added.  Normally this should be
           the same gob.

           `remove_gobs`: a dictionary mapping from primary key to the
           gob to be removed under that key.

           `add_keys`: a set of tuples of the key and the primary keys
           of the gobs to be referenced by that key.

           `remove_keys`: a set of tuples of the key and the primary
           keys of the gobs to no longer be referenced by that key.

           `add_unique_keys`: a dictionary mapping from the keys to be
           added to the primary keys of the object to stored at that
           key.

           `update_unique_keys`: a dictionary mapping from the keys to
           be updated to a tuple containing the primary key of the
           object previously stored and the object subsequently to be
           stored at that key.

           `remove_unique_keys`: a dictionary mapping from the keys to
           be removed to the primary keys of the object previously
           stored at that key.

           `collection_additions`: a set of keys to be initialized.

           `collection_removals`: a set of keys to be removed from the
           database.

           `conditions`: a dictionary mapping from the primary keys of
           the objects which call for the conditions to the conditions
           themselves.

           `affected_keys`: a set of all keys that will be directly or
           indirectly affected by the commit.
        """
        pass

    def _dissociate_key(self, key, use_persisted_version=False):
        """Duplicate the key, dissociating it from any gobs or fields
        with which it had been created."""
        ret = []
        for f in key:
            f
            if isinstance(f, gobpersist.field.Field):
                f = f.clone(clean_break=True)
                if use_persisted_version:
                    f._set(f.persisted_value)
            ret.append(f)
        return tuple(ret)

    def commit(self, additions=[], updates=[], removals=[],
               collection_additions=[], collection_removals=[]):
        add_gobs = {}
        update_gobs = {}
        remove_gobs = {}
        add_keys = set()
        remove_keys = set()
        add_unique_keys = {}
        update_unique_keys = {}
        remove_unique_keys = {}
        collection_additions = set([self._dissociate_key(key) for key in collection_additions])
        collection_removals = set([self._dissociate_key(key) for key in collection_removals]) - collection_additions
        conditions = {}
        affected_keys = collection_additions | collection_removals

        # process all removals first
        for removal in itertools.chain(removals, updates):
            gob = removal['gob']
            old_obj_key = self._dissociate_key(gob.obj_key, True)
            affected_keys.add(old_obj_key)
            if 'remove_unique_keys' in removal:
                for key in removal['remove_unique_keys']:
                    key = self._dissociate_key(key, True)
                    affected_keys.add(key)
                    remove_unique_keys[key] = old_obj_key
            if 'remove_keys' in removal:
                for key in removal['remove_keys']:
                    key = self._dissociate_key(key)
                    affected_keys.add(key)
                    remove_keys.add((key, obj_key))
            for key in gob.unique_keyset(True):
                key = self._dissociate_key(key, True)
                affected_keys.add(key)
                remove_unique_keys[key] = old_obj_key
            for key in gob.keyset(True):
                key = self._dissociate_key(key, True)
                affected_keys.add(key)
                remove_keys.add((key, old_obj_key))
            remove_gobs[old_obj_key] = gob
            if 'conditions' in removal:
                conditions[old_obj_key] = removal['conditions']

        # now process additions/updates
        for addition in itertools.chain(additions, updates):
            gob = addition['gob']
            obj_key = self._dissociate_key(gob.obj_key)
            affected_keys.add(obj_key)
            if 'add_unique_keys' in addition:
                for key in addition['add_unique_keys']:
                    key = self._dissociate_key(key)
                    affected_keys.add(key)
                    if key in add_unique_keys \
                            and add_unique_keys[key] != obj_key:
                        # badness...
                        raise gobpersist.exception.Corruption(
                            "Duplicate addition of "
                            "unique key %s detected" % repr(key))
                    add_unique_keys[key] = obj_key
            if 'add_keys' in addition:
                for key in addition['add_keys']:
                    key = self._dissociate_key(key)
                    affected_keys.add(key)
                    if key in collection_removals:
                        collection_removals.remove(key)
                    add_keys.add((key, obj_key))
            for key in gob.unique_keyset():
                key = self._dissociate_key(key)
                affected_keys.add(key)
                if key in add_unique_keys \
                        and add_unique_keys[key] != obj_key:
                    # badness...
                    raise gobpersist.exception.Corruption(
                        "Duplicate addition of "
                        "unique key %s detected" % repr(key))
                add_unique_keys[key] = obj_key
            for key in gob.keyset():
                key = self._dissociate_key(key)
                if key in collection_removals:
                    collection_removals.remove(key)
                add_keys.add((key, obj_key))
            add_gobs[obj_key] = gob

        # harmonize additions and removals and produce updates
        add_keys, remove_keys \
            = add_keys - remove_keys, remove_keys - add_keys
        for key in add_unique_keys.keys():
            if key in remove_unique_keys:
                update_unique_keys[key] = (remove_unique_keys[key], add_unique_keys[key])
                del add_unique_keys[key]
                del remove_unique_keys[key]
        for key in add_gobs.keys():
            if key in remove_gobs:
                update_gobs[key] = (remove_gobs[key], add_gobs[key])
                del add_gobs[key]
                del remove_gobs[key]

        return self.kv_commit(add_gobs, update_gobs, remove_gobs, add_keys, remove_keys,
                              add_unique_keys, update_unique_keys, remove_unique_keys,
                              collection_additions, collection_removals,
                              conditions, affected_keys)
