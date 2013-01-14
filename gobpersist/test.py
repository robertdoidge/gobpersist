#!/usr/bin/env python
# test.py - Unit tests for Gobpersist
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

# FIXME: we need to test the various initialize_db functions

import sys
if __name__ == '__main__':
    import os.path

    libdir = os.path.join(os.path.dirname(__file__), '..')
    sys.path.insert(0, libdir)

import unittest
import datetime
import iso8601
import uuid
import warnings

import hashlib
import operator

import gobpersist.gob
import gobpersist.field
import gobpersist.schema
import gobpersist.session
import gobpersist.storage
import gobpersist.exception
import gobpersist.backends.memcached

warnings.simplefilter('default')

sys.setrecursionlimit(4000)

def get_gob_class():
    class GobTest(gobpersist.gob.Gob):
        boolean_field = gobpersist.field.BooleanField()
        datetime_field = gobpersist.field.DateTimeField()
        timestamp_field = gobpersist.field.TimestampField(unique=True)
        string_field = gobpersist.field.StringField(encoding='UTF-8')
        integer_field = gobpersist.field.IntegerField()
        incrementing_field = gobpersist.field.IncrementingField()
        real_field = gobpersist.field.RealField()
        enum_field = gobpersist.field.EnumField(choices=('test1','test2'))
        uuid_field = gobpersist.field.UUIDField()
        list_field = gobpersist.field.ListField(element_type=gobpersist.field.IntegerField())
        set_field = gobpersist.field.SetField(element_type=gobpersist.field.IntegerField())

        def keyset(self, use_persisted_version=False):
            if (self.parent_key.persisted_value
                        if use_persisted_version
                    else self.parent_key) == None:
                return [self.coll_key] + self.keys
            else:
                return self.keys

        my_key = gobpersist.field.UUIDField(primary_key=True)
        parent_key = gobpersist.field.UUIDField(null=True)

        parent = gobpersist.field.ForeignObject(foreign_class='self',
                                                local_field='parent_key',
                                                foreign_field='primary_key',
                                                key=('gobtests', parent_key))
        children = gobpersist.field.ForeignCollection(foreign_class='self',
                                                      local_field='primary_key',
                                                      foreign_field='parent_key',
                                                      key=('gobtests', my_key,
                                                           'children'))

        keys = [('gobtests', parent_key, 'children')]
        unique_keys = [('gobtests_by_timestamp', timestamp_field)]

        consistency=[{
                'field': my_key,
                'foreign_class': 'self',
                'foreign_obj': ('gobtests', my_key, 'children'),
                'foreign_field': 'parent_key',
                'update': 'cascade',
                'remove': 'cascade'
                }]

    return GobTest

def get_schema_class():
    class SchemaTest(gobpersist.schema.Schema):
        gobtests = get_gob_class()
    return SchemaTest

def get_memcached():
    return gobpersist.backends.memcached.MemcachedBackend(expiry=60)

def get_session():
    return gobpersist.session.Session(backend=get_memcached())

class Initialization(unittest.TestCase):
    def test_gob_definition(self):
        gob_class = get_gob_class()
        assert(issubclass(gob_class, gobpersist.gob.Gob))
        assert(gob_class.primary_key is gob_class.my_key)
        assert(gob_class.class_key == 'gobtests')
        assert(len(gob_class.obj_key) == 2
               and gob_class.obj_key[0] == 'gobtests'
               and gob_class.obj_key[1] is gob_class.my_key)
        assert(gob_class.coll_key == ('gobtests',))
        assert(gob_class.boolean_field.name == 'boolean_field')
        assert(gob_class.datetime_field.name == 'datetime_field')
        assert(gob_class.timestamp_field.name == 'timestamp_field')
        assert(gob_class.string_field.name == 'string_field')
        assert(gob_class.integer_field.name == 'integer_field')
        assert(gob_class.incrementing_field.name == 'incrementing_field')
        assert(gob_class.real_field.name == 'real_field')
        assert(gob_class.enum_field.name == 'enum_field')
        assert(gob_class.uuid_field.name == 'uuid_field')
        assert(gob_class.list_field.name == 'list_field')
        assert(gob_class.set_field.name == 'set_field')
        assert(gob_class.my_key.name == 'my_key')
        assert(gob_class.parent_key.name == 'parent_key')
        assert(gob_class.parent.name == 'parent')
        assert(gob_class.children.name == 'children')
        assert(gob_class.boolean_field._name == 'boolean_field')
        assert(gob_class.datetime_field._name == 'datetime_field')
        assert(gob_class.timestamp_field._name == 'timestamp_field')
        assert(gob_class.string_field._name == 'string_field')
        assert(gob_class.integer_field._name == 'integer_field')
        assert(gob_class.incrementing_field._name == 'incrementing_field')
        assert(gob_class.real_field._name == 'real_field')
        assert(gob_class.enum_field._name == 'enum_field')
        assert(gob_class.uuid_field._name == 'uuid_field')
        assert(gob_class.list_field._name == 'list_field')
        assert(gob_class.set_field._name == 'set_field')
        assert(gob_class.my_key._name == 'my_key')
        assert(gob_class.parent_key._name == 'parent_key')
        assert(gob_class.parent._name == 'parent')
        assert(gob_class.children._name == 'children')
        assert(gob_class.parent.foreign_class is gob_class)
        assert(gob_class.children.foreign_class is gob_class)
        assert(len(gob_class.consistency) == 1
               and gob_class.consistency[0]['foreign_class'] is gob_class)

    def test_gob_reload(self):
        gob_class = get_gob_class()
        gob_class.new_field = gobpersist.field.IntegerField()
        gob_class.reload_class()
        assert(gob_class.new_field.name == 'new_field')
        assert(gob_class.new_field._name == 'new_field')

    def test_schema_definition(self):
        sc_class = get_schema_class()

    def test_session_creation(self):
        s = get_session()

    def test_schema_creation(self):
        sc_class = get_schema_class()
        sc = sc_class(session=get_session())
        assert(isinstance(sc.gobtests, gobpersist.schema.SchemaCollection))
        assert(sc.gobtests.cls is sc_class.gobtests)
        assert(sc.gobtests.session is sc.session)
        assert(sc.gobtests.key == sc_class.gobtests.coll_key)

    def test_gob_creation(self):
        gob_class = get_gob_class()
        s = get_session()
        gob = gob_class(session=s)
        assert(gob.boolean_field is not gob_class.boolean_field)
        assert(gob.datetime_field is not gob_class.datetime_field)
        assert(gob.timestamp_field is not gob_class.timestamp_field)
        assert(gob.string_field is not gob_class.string_field)
        assert(gob.integer_field is not gob_class.integer_field)
        assert(gob.incrementing_field is not gob_class.incrementing_field)
        assert(gob.real_field is not gob_class.real_field)
        assert(gob.enum_field is not gob_class.enum_field)
        assert(gob.uuid_field is not gob_class.uuid_field)
        assert(gob.list_field is not gob_class.list_field)
        assert(gob.set_field is not gob_class.set_field)
        assert(gob.my_key is not gob_class.my_key)
        assert(gob.parent_key is not gob_class.parent_key)
        assert(gob.parent is not gob_class.parent)
        assert(gob.children is not gob_class.children)
        assert(gob.boolean_field.instance == gob)
        assert(gob.datetime_field.instance == gob)
        assert(gob.timestamp_field.instance == gob)
        assert(gob.string_field.instance == gob)
        assert(gob.integer_field.instance == gob)
        assert(gob.incrementing_field.instance == gob)
        assert(gob.real_field.instance == gob)
        assert(gob.enum_field.instance == gob)
        assert(gob.uuid_field.instance == gob)
        assert(gob.list_field.instance == gob)
        assert(gob.set_field.instance == gob)
        assert(gob.my_key.instance == gob)
        assert(gob.parent_key.instance == gob)
        assert(gob.parent.instance == gob)
        assert(gob.children.instance == gob)
        assert(gob.primary_key is gob.my_key)
        assert(len(gob.obj_key) == 2
               and gob.obj_key[0] == 'gobtests'
               and gob.obj_key[1] is gob.my_key)
        assert(len(gob.keys) == 1
               and len(gob.keys[0]) == 3
               and gob.keys[0][0] == 'gobtests'
               and gob.keys[0][1] is gob.parent_key
               and gob.keys[0][2] == 'children')
        assert(len(gob.unique_keys) == 1
               and len(gob.unique_keys[0]) == 2
               and gob.unique_keys[0][0] == 'gobtests_by_timestamp'
               and gob.unique_keys[0][1] is gob.timestamp_field)
        assert(len(gob.consistency) == 1
               and gob.consistency[0]['field'] is gob.my_key)
        assert(len(gob.consistency[0]['foreign_obj']) == 3
               and gob.consistency[0]['foreign_obj'][0] == 'gobtests'
               and gob.consistency[0]['foreign_obj'][1] is gob.my_key
               and gob.consistency[0]['foreign_obj'][2] is 'children')

class TestWithSchema(unittest.TestCase):
    def setUp(self):
        super(TestWithSchema, self).setUp()
        self.sc_class = get_schema_class()
        self.sc = self.sc_class(session=get_session())

class TestWithGob(TestWithSchema):
    gob_key = str(uuid.uuid4())
    gob2_key = str(uuid.uuid4())

    def setUp(self):
        super(TestWithGob, self).setUp()
        self.gob = self.sc_class.gobtests(self.sc)
        self.gob.boolean_field.set(True)
        self.gob.datetime_field = datetime.datetime.utcnow()
        self.gob.string_field = 'example string'
        self.gob.integer_field = 2
        self.gob.real_field = 3.1415
        self.gob.enum_field = 'test2'
        self.gob.list_field = [1, 2, 3]
        self.gob.set_field = set([1, 2, 3])
        self.gob.uuid_field = str(uuid.uuid4())
        self.gob.primary_key = self.gob_key
        self.gob.parent_key = None
        self.gob2 = self.sc_class.gobtests(self.sc)
        self.gob2.boolean_field.set(True)
        self.gob2.datetime_field = datetime.datetime.utcnow()
        self.gob2.string_field = 'example string 2'
        self.gob2.integer_field = 97
        self.gob2.real_field = 1.618
        self.gob2.enum_field = 'test2'
        self.gob2.list_field = [1, 2, 3]
        self.gob2.set_field = set([1, 2, 3])
        self.gob2.primary_key = self.gob2_key
        self.gob2.parent_key = self.gob_key

class TestGob(TestWithGob):
    def test_keyset(self):
        keys = self.gob.keyset()
        assert(len(keys) == 2)
        assert(('gobtests',) in keys)
        assert(('gobtests', self.gob.parent_key, 'children') in keys)

    def test_unique_keyset(self):
        unique_keys = self.gob.unique_keyset()
        assert(len(unique_keys) == 1)
        assert(('gobtests_by_timestamp', self.gob.timestamp_field) in unique_keys)

    def test_save(self):
        self.gob.save()
        self.sc.commit()
        try:
            gotten_gob = self.sc.gobtests.get(self.gob_key)
            assert(gotten_gob.primary_key == self.gob.primary_key)
        finally:
            self.gob.remove()
            self.sc.commit()

    def test_remove(self):
        self.gob.save()
        self.sc.commit()
        self.gob.remove()
        self.sc.commit()
        self.assertRaises(gobpersist.exception.NotFound,
                          self.sc.gobtests.get, self.gob_key)

    def test_revert(self):
        self.gob.save()
        self.sc.commit()
        try:
            assert(self.gob.string_field == 'example string')
            self.gob.string_field = 'changed example string'
            assert(self.gob.string_field == 'changed example string')
            self.gob.revert()
            assert(self.gob.string_field == 'example string')
        finally:
            self.gob.remove()
            self.sc.commit()

    def test_repr(self):
        s = repr(self.gob)
        assert(isinstance(s, (str, unicode)))

    def test_str(self):
        s = str(self.gob)
        assert(isinstance(s, str))

class TestSchemaCollection(TestWithGob):
    def test_list(self):
        self.gob.save()
        self.gob2.save()
        self.sc.commit()
        try:
            r = self.sc.gobtests.list()
            assert(len(r) == 1)
            assert(r[0].primary_key == self.gob_key)
            r = self.sc.gobtests.list(integer_field=2, string_field='example string')
            assert(len(r) == 1)
            assert(r[0].primary_key == self.gob_key)
            r = self.sc.gobtests.list(integer_field=('lt', 101))
            assert(len(r) == 1)
            assert(r[0].primary_key == self.gob_key)
            r = self.sc.gobtests.list(integer_field=('gt', 3))
            assert(len(r) == 0)
        finally:
            self.gob.remove()
            self.gob2.remove()
            self.sc.commit()

    def test_get(self):
        self.gob.save()
        self.gob2.save()
        self.sc.commit()
        try:
            r = self.sc.gobtests.get(self.gob_key)
            assert(r.primary_key == self.gob_key)
            r = self.sc.gobtests.get(self.gob2_key)
            assert(r.primary_key == self.gob2_key)
        finally:
            self.gob.remove()
            self.gob2.remove()
            self.sc.commit()
        self.assertRaises(gobpersist.exception.NotFound,
                          self.sc.gobtests.get, self.gob_key)
        self.assertRaises(gobpersist.exception.NotFound,
                          self.sc.gobtests.get, self.gob2_key)

    def test_add(self):
        self.sc.gobtests.add(self.gob)
        self.sc.commit()
        try:
            r = self.sc.gobtests.get(self.gob_key)
            assert(r.primary_key == self.gob_key)
        finally:
            self.gob.remove()
            self.sc.commit()

    def test_update(self):
        self.gob.save()
        self.sc.commit()
        try:
            self.gob.string_field = 'changed example string'
            self.sc.gobtests.update(self.gob)
            self.sc.commit()
            r = self.sc.gobtests.get(self.gob_key)
            assert(r.string_field == 'changed example string'
                   and not r.string_field.dirty)
        finally:
            self.gob.remove()
            self.sc.commit()

    def test_remove(self):
        self.gob.save()
        self.sc.commit()
        self.sc.gobtests.remove(self.gob)
        self.sc.commit()
        self.assertRaises(gobpersist.exception.NotFound,
                          self.sc.gobtests.get, self.gob_key)

    def test_repr(self):
        s = repr(self.sc.gobtests)
        assert(isinstance(s, (str, unicode)))

    def test_str(self):
        s = str(self.sc.gobtests)
        assert(isinstance(s, str))

class TestSchema(TestWithGob):
    def test_collection_for_key(self):
        self.gob2.save()
        self.sc.commit()
        try:
            sc2 = self.sc.collection_for_key(self.sc_class.gobtests,
                                             ('gobtests', self.gob_key, 'children'))
            r = sc2.list()
            assert(len(r) == 1)
            assert(r[0].primary_key == self.gob2_key)
        finally:
            self.gob2.remove()
            self.sc.commit()

    def test_repr(self):
        s = repr(self.sc)
        assert(isinstance(s, (str, unicode)))

    def test_str(self):
        s = str(self.sc)
        assert(isinstance(s, str))

class TestSession(TestWithGob):
    def test_add(self):
        self.sc.session.add(self.gob)
        self.sc.commit()
        try:
            r = self.sc.gobtests.get(self.gob_key)
            assert(r.primary_key == self.gob_key)
        finally:
            self.gob.remove()
            self.sc.commit()

    def test_update(self):
        self.gob.save();
        self.sc.commit();
        try:
            self.gob.string_field = 'changed example string'
            self.sc.session.update(self.gob)
            self.sc.commit()
            r = self.sc.gobtests.get(self.gob_key)
            assert(r.string_field == 'changed example string'
                   and not r.string_field.dirty)
        finally:
            self.gob.remove()
            self.sc.commit()

    def test_remove(self):
        self.gob.save()
        self.sc.commit()
        self.sc.session.remove(self.gob)
        self.sc.commit()
        self.assertRaises(gobpersist.exception.NotFound,
                          self.sc.gobtests.get, self.gob_key)

    def add_collection(self):
        self.sc.session.add_collection(('gobtests-notused',))
        self.sc.commit()
        try:
            r = self.sc.query(self.sc_class.gobtests, ('gobtests-notused'))
            assert(len(r) == 0)
        finally:
            self.sc.session.remove_collection(('gobtests-notused',))
            self.sc.commit()

    def remove_collection(self):
        self.sc.session.add_collection(('gobtests-notused',))
        self.sc.commit()
        self.sc.session.remove_collection(('gobtests-notused',))
        self.sc.commit()
        self.assertRaises(gobpersist.exception.NotFound,
                          self.sc.query,
                          self.sc_class.gobtests,
                          ('gobtests-notused'))

    def test_rollback(self):
        self.gob.save()
        assert(self.gob in self.sc.operations['additions'])
        self.sc.session.rollback()
        assert(self.gob not in self.sc.operations['additions'])
        self.gob.save()
        self.sc.commit()
        try:
            self.gob.string_field = 'changed example string'
            assert(self.gob.string_field == 'changed example string')
            self.gob.save()
            assert(self.gob in self.sc.operations['updates'])
            self.sc.session.rollback(revert=True)
            assert(self.gob not in self.sc.operations['updates'])
            assert(self.gob.string_field == 'example string')
        finally:
            self.gob.remove()
            self.sc.commit()

    def test_start_transaction(self):
        self.gob.save()
        assert(self.gob in self.sc.operations['additions'])
        assert(self.gob2 not in self.sc.operations['additions'])
        self.sc.session.start_transaction()
        self.gob2.save()
        assert(self.gob not in self.sc.operations['additions'])
        assert(self.gob2 in self.sc.operations['additions'])
        self.sc.rollback()
        assert(self.gob in self.sc.operations['additions'])
        assert(self.gob2 not in self.sc.operations['additions'])
        self.sc.session.start_transaction()
        self.gob2.save()
        self.sc.commit()
        assert(self.gob in self.sc.operations['additions'])
        assert(self.gob2 in self.sc.operations['additions'])
        self.sc.rollback()
        assert(self.gob not in self.sc.operations['additions'])
        assert(self.gob2 not in self.sc.operations['additions'])

    def test_commit(self):
        pass

    def test_repr(self):
        s = repr(self.sc.session)
        assert(isinstance(s, (str, unicode)))

    def test_str(self):
        s = str(self.sc.session)
        assert(isinstance(s, str))

class TestQuery(TestWithGob):
    def setUp(self):
        super(TestQuery, self).setUp()
        self.gob_cls = self.sc_class.gobtests
        self.gob.save()
        self.gob2.save()
        self.sc.commit()

    def tearDown(self):
        self.gob.remove()
        self.gob2.remove()
        self.sc.commit()

    def test_key_only(self):
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key))
        assert(len(r) == 1)
        assert(r[0].primary_key == self.gob_key)

    def test_key_range_only(self):
        # nothing to test this with
        pass

    def test_retrieve(self):
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                          retrieve=['boolean_field', 'real_field'])
        assert(len(r) == 1)
        assert(r[0].primary_key == self.gob_key)
        assert(r[0].boolean_field == True)
        assert(r[0].real_field == 3.1415)

    def test_order(self):
        r = self.sc.query(self.gob_cls,
                          key=('gobtests', self.gob_key), order=[{'asc': 'boolean_field'},
                                                   {'desc': 'real_field'}])
        assert(len(r) == 1)
        assert(r[0].primary_key == self.gob_key)

    def test_offset(self):
        r = self.sc.query(self.gob_cls,
                          key=('gobtests', self.gob_key), offset=0)
        assert(len(r) == 1)
        assert(r[0].primary_key == self.gob_key)
        r = self.sc.query(self.gob_cls,
                          key=('gobtests', self.gob_key), offset=1)
        assert(len(r) == 0)

    def test_limit(self):
        r = self.sc.query(self.gob_cls,
                          key=('gobtests', self.gob_key), limit=2)
        assert(len(r) == 1)
        assert(r[0].primary_key == self.gob_key)
        r = self.sc.query(self.gob_cls,
                          key=('gobtests', self.gob_key), limit=1)
        assert(len(r) == 1)
        assert(r[0].primary_key == self.gob_key)
        r = self.sc.query(self.gob_cls,
                          key=('gobtests', self.gob_key), limit=0)
        assert(len(r) == 0)

    def test_comparison(self):
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                          query={'eq': [('real_field',), 3.1415]})
        assert(len(r) == 1)
        assert(r[0].primary_key == self.gob_key)
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                          query={'eq': [('real_field',), 1.618]})
        assert(len(r) == 0)
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                          query={'ne': [('real_field',), 1.618]})
        assert(len(r) == 1)
        assert(r[0].primary_key == self.gob_key)
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                          query={'ne': [('real_field',), 3.1415]})
        assert(len(r) == 0)
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                          query={'gt': [('real_field',), 1.618]})
        assert(len(r) == 1)
        assert(r[0].primary_key == self.gob_key)
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                          query={'gt': [('real_field',), 3.1415]})
        assert(len(r) == 0)
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                          query={'gt': [('real_field',), 6.2831]})
        assert(len(r) == 0)
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                          query={'lt': [('real_field',), 6.2831]})
        assert(len(r) == 1)
        assert(r[0].primary_key == self.gob_key)
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                          query={'lt': [('real_field',), 3.1415]})
        assert(len(r) == 0)
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                          query={'lt': [('real_field',), 1.618]})
        assert(len(r) == 0)
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                          query={'ge': [('real_field',), 1.618]})
        assert(len(r) == 1)
        assert(r[0].primary_key == self.gob_key)
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                          query={'ge': [('real_field',), 3.1415]})
        assert(len(r) == 1)
        assert(r[0].primary_key == self.gob_key)
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                          query={'ge': [('real_field',), 6.2831]})
        assert(len(r) == 0)
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                          query={'le': [('real_field',), 6.2831]})
        assert(len(r) == 1)
        assert(r[0].primary_key == self.gob_key)
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                          query={'le': [('real_field',), 3.1415]})
        assert(len(r) == 1)
        assert(r[0].primary_key == self.gob_key)
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                          query={'le': [('real_field',), 1.618]})
        assert(len(r) == 0)

    def test_boolean(self):
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                          query={'and': [
                    {'eq': [('real_field',), 3.1415]},
                    {'eq': [('integer_field',), 2]}]})
        assert(len(r) == 1)
        assert(r[0].primary_key == self.gob_key)
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                          query={'and': [
                    {'eq': [('real_field',), 3.1415]},
                    {'eq': [('integer_field',), 97]}]})
        assert(len(r) == 0)
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                          query={'and': [
                    {'eq': [('real_field',), 1.618]},
                    {'eq': [('integer_field',), 97]}]})
        assert(len(r) == 0)
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                          query={'or': [
                    {'eq': [('real_field',), 3.1415]},
                    {'eq': [('integer_field',), 2]}]})
        assert(len(r) == 1)
        assert(r[0].primary_key == self.gob_key)
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                          query={'or': [
                    {'eq': [('real_field',), 3.1415]},
                    {'eq': [('integer_field',), 97]}]})
        assert(len(r) == 1)
        assert(r[0].primary_key == self.gob_key)
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                          query={'or': [
                    {'eq': [('real_field',), 1.618]},
                    {'eq': [('integer_field',), 97]}]})
        assert(len(r) == 0)
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                          query={'nor': [
                    {'eq': [('real_field',), 3.1415]},
                    {'eq': [('integer_field',), 2]}]})
        assert(len(r) == 0)
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                          query={'nor': [
                    {'eq': [('real_field',), 3.1415]},
                    {'eq': [('integer_field',), 97]}]})
        assert(len(r) == 0)
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                          query={'nor': [
                    {'eq': [('real_field',), 1.618]},
                    {'eq': [('integer_field',), 97]}]})
        assert(len(r) == 1)
        assert(r[0].primary_key == self.gob_key)

    def test_nested_scalar(self):
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob2_key),
                          query={'eq': [('parent', 'real_field'), 3.1415]})
        assert(len(r) == 1)
        assert(r[0].primary_key == self.gob2_key)
        r = self.sc.query(self.gob_cls, key=('gobtests', self.gob2_key),
                          query={'eq': [('parent', 'real_field'), 1.618]})
        assert(len(r) == 0)

    def test_quantifiers(self):
        gob3 = self.sc_class.gobtests(self.sc)
        gob3.boolean_field.set(True)
        gob3.datetime_field = datetime.datetime.utcnow()
        gob3.string_field = 'example string 3'
        gob3.integer_field = 97
        gob3.real_field = 6.2831
        gob3.enum_field = 'test2'
        gob3.list_field = [1, 2, 3]
        gob3.set_field = set([1, 2, 3])
        gob3.uuid_field = str(uuid.uuid4())
        gob3.primary_key = str(uuid.uuid4())
        gob3.parent_key = self.gob_key
        gob3.save()
        self.sc.commit()
        try:
            r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                              query={'eq': [
                        {'any': ('children', 'real_field')},
                        6.2831]})
            assert(len(r) == 1)
            assert(r[0].primary_key == self.gob_key)
            r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                              query={'eq': [
                        {'any': ('children', 'integer_field')},
                        97]})
            assert(len(r) == 1)
            assert(r[0].primary_key == self.gob_key)
            r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                              query={'eq': [
                        {'any': ('children', 'real_field')},
                        3.1415]})
            assert(len(r) == 0)
            r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                              query={'eq': [
                        {'all': ('children', 'real_field')},
                        6.2831]})
            assert(len(r) == 0)
            r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                              query={'eq': [
                        {'all': ('children', 'integer_field')},
                        97]})
            assert(len(r) == 1)
            assert(r[0].primary_key == self.gob_key)
            r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                              query={'eq': [
                        {'all': ('children', 'real_field')},
                        3.1415]})
            assert(len(r) == 0)
            r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                              query={'eq': [
                        {'none': ('children', 'real_field')},
                        6.2831]})
            assert(len(r) == 0)
            r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                              query={'eq': [
                        {'none': ('children', 'integer_field')},
                        97]})
            assert(len(r) == 0)
            r = self.sc.query(self.gob_cls, key=('gobtests', self.gob_key),
                              query={'eq': [
                        {'none': ('children', 'real_field')},
                        3.1415]})
            assert(len(r) == 1)
            assert(r[0].primary_key == self.gob_key)
        finally:
            gob3.remove()
            self.sc.commit()


class TestStorage(TestWithGob):
    # currently no supported storage engine with which to test...
    pass

class TestField(TestWithGob):
    def setUp(self):
        super(TestField, self).setUp()
        self.field = self.gob.boolean_field
        self.test_v = True
        self.test_less = False

    def test_set(self):
        old_value = self.field.value
        self.field.set(self.test_v)
        assert(self.field == self.test_v)
        assert(self.field.dirty)
        assert(self.field.has_value)

    def test_clone(self):
        new_field = self.field.clone()
        assert(new_field is not self.field)
        assert(new_field == self.field)
        assert(new_field.instance is self.field.instance)
        new_field = self.field.clone(clean_break = True)
        assert(new_field is not self.field)
        assert(new_field == self.field)
        assert(new_field.instance is None)

    def test_repr(self):
        s = repr(self.field)
        assert(isinstance(s, (str, unicode)))

    def test_str(self):
        s = str(self.field)
        assert(isinstance(s, str))

    def test_comparison(self):
        self.field.set(self.test_v)
        assert(self.field > self.test_less)
        assert(self.field >= self.test_less)
        assert(not self.field < self.test_less)
        assert(not self.field <= self.test_less)
        assert(not self.field == self.test_less)
        assert(self.field != self.test_less)
        assert(cmp(self.field, self.test_less) > 0)

    def test_hash(self):
        h = hash(self.field)
        assert(isinstance(h, int))
        self.assertRaises(AssertionError, self.field.set, self.test_v)

    def test_nonzero(self):
        assert(self.field or not self.field)

    def test_null(self):
        self.assertRaises(ValueError, self.field.set, None)

class TestBooleanField(TestField):
    def setUp(self):
        super(TestBooleanField, self).setUp()
        self.field = self.gob.boolean_field
        self.test_v = True
        self.test_less = False

    def test_validate(self):
        self.assertRaises(TypeError, self.field.set, 5)
        self.field.set(True)

class TestDateTimeField(TestField):
    def setUp(self):
        super(TestDateTimeField, self).setUp()
        self.field = self.gob.datetime_field
        self.test_v = datetime.datetime.utcnow()
        self.test_less = self.test_v - datetime.timedelta(days=1)

    def test_validate(self):
        self.assertRaises(TypeError, self.field.set, 5)
        self.field.set(datetime.datetime.utcnow())
        self.field.set('1054-07-04T00:00:00')

class TestTimestampField(TestDateTimeField):
    def setUp(self):
        super(TestTimestampField, self).setUp()
        self.field = self.gob.timestamp_field
        self.test_v = datetime.datetime.utcnow()
        self.test_less = self.test_v - datetime.timedelta(days=1)

class TestStringField(TestField):
    def setUp(self):
        super(TestStringField, self).setUp()
        self.field = self.gob.string_field
        self.test_v = 'example string'
        self.test_less = 'example strin'

    def test_validate(self):
        self.assertRaises(TypeError, self.field.set, 5)
        self.field.set('abc')
        self.field.set(unicode('abc'))

    def test_unicode(self):
        u = unicode(self.field)
        assert(isinstance(u, unicode))

    def test_decode(self):
        u = self.field.decode()
        assert(isinstance(u, unicode))

    def test_encode(self):
        s = self.field.encode()
        assert(isinstance(s, str))
        s = self.field.encode(encoding='UTF-16')
        assert(isinstance(s, str))

    def test_len(self):
        self.field.set(self.test_v)
        assert(len(self.field) == len(self.test_v))

    def test_getitem(self):
        self.field.set(self.test_v)
        assert(self.field[1] == self.test_v[1])

    def test_iter(self):
        self.field.set(self.test_v)
        i = 0
        for c in self.field:
            assert(c == self.field[i])
            i += 1

    def test_reversed(self):
        self.field.set(self.test_v)
        i = len(self.test_v) - 1
        for c in reversed(self.field):
            assert(c == self.field[i])
            i -= 1

    def test_contains(self):
        self.field.set(self.test_v)
        assert(self.test_v[1] in self.field)

    def test_add(self):
        self.field.set(self.test_v)
        assert(self.field + self.test_less == self.test_v + self.test_less)

    def test_radd(self):
        self.field.set(self.test_v)
        assert(self.test_less + self.field == self.test_less + self.test_v)

    def test_iadd(self):
        self.field.set(self.test_v)
        self.field += self.test_less
        assert(self.field == self.test_v + self.test_less)

    def test_mul(self):
        self.field.set(self.test_v)
        assert(self.field * 3 == self.test_v * 3)

    def test_rmul(self):
        self.field.set(self.test_v)
        assert(3 * self.field == 3 * self.test_v)

    def test_imul(self):
        self.field.set(self.test_v)
        self.field *= 3
        assert(self.field == self.test_v * 3)

# doesn't inherit from unit tests, since this shouldn't get run by itself...
class TestNumericField(object):
    def setUp(self):
        pass
    def test_add(self):
        self.field.set(self.test_v)
        assert(self.field + self.test_less == self.test_v + self.test_less)
    def test_sub(self):
        self.field.set(self.test_v)
        assert(self.field - self.test_less == self.test_v - self.test_less)
    def test_mul(self):
        self.field.set(self.test_v)
        assert(self.field * self.test_less == self.test_v * self.test_less)
    def test_div(self):
        self.field.set(self.test_v)
        assert(self.field / self.test_less == self.test_v / self.test_less)
    def test_truediv(self):
        self.field.set(self.test_v)
        assert(self.field / self.test_less == self.test_v / self.test_less)
    def test_floordiv(self):
        self.field.set(self.test_v)
        assert(self.field // self.test_less == self.test_v // self.test_less)
    def test_mod(self):
        self.field.set(self.test_v)
        assert(self.field % self.test_less == self.test_v % self.test_less)
    def test_divmod(self):
        self.field.set(self.test_v)
        assert(divmod(self.field, self.test_less)
               == divmod(self.test_v, self.test_less))
    def test_pow(self):
        self.field.set(self.test_v)
        assert(self.field**self.test_less == self.test_v**self.test_less)

    def test_radd(self):
        self.field.set(self.test_v)
        assert(self.test_less + self.field == self.test_less + self.test_v)
    def test_rsub(self):
        self.field.set(self.test_v)
        assert(self.test_less - self.field == self.test_less - self.test_v)
    def test_rmul(self):
        self.field.set(self.test_v)
        assert(self.test_less * self.field == self.test_less * self.test_v)
    def test_rdiv(self):
        self.field.set(self.test_v)
        assert(self.test_less / self.field == self.test_less / self.test_v)
    def test_rtruediv(self):
        self.field.set(self.test_v)
        assert(self.test_less / self.field == self.test_less / self.test_v)
    def test_rfloordiv(self):
        self.field.set(self.test_v)
        assert(self.test_less // self.field == self.test_less // self.test_v)
    def test_rmod(self):
        self.field.set(self.test_v)
        assert(self.test_less % self.field == self.test_less % self.test_v)
    def test_rdivmod(self):
        self.field.set(self.test_v)
        assert(divmod(self.test_less, self.field)
               == divmod(self.test_less, self.test_v))
    def test_rpow(self):
        self.field.set(self.test_v)
        assert(self.test_less**self.field == self.test_less**self.test_v)

    def test_iadd(self):
        self.field.set(self.test_v)
        self.field += self.test_less
        assert(self.field == self.test_v + self.test_less)
    def test_isub(self):
        self.field.set(self.test_v)
        self.field -= self.test_less
        assert(self.field == self.test_v - self.test_less)
    def test_imul(self):
        self.field.set(self.test_v)
        self.field *= self.test_less
        assert(self.field == self.test_v * self.test_less)
    def test_idiv(self):
        self.field.set(self.test_v)
        self.field /= self.test_less
        assert(self.field == self.test_v / self.test_less)
    def test_itruediv(self):
        self.field.set(self.test_v)
        self.field /= self.test_less
        assert(self.field == self.test_v / self.test_less)
    def test_ifloordiv(self):
        self.field.set(self.test_v)
        self.field //= self.test_less
        assert(self.field == self.test_v // self.test_less)
    def test_ipow(self):
        self.field.set(self.test_v)
        self.field **= self.test_less
        assert(self.field == self.test_v**self.test_less)

    def test_neg(self):
        self.field.set(self.test_v)
        assert(-self.field == -self.test_v)
    def test_pos(self):
        self.field.set(self.test_v)
        assert(+self.field == +self.test_v)
    def test_abs(self):
        self.field.set(-self.test_v)
        assert(abs(self.field) == abs(-self.test_v))
    def test_complex(self):
        self.field.set(-self.test_v)
        assert(complex(self.field) == complex(-self.test_v))
    def test_int(self):
        self.field.set(-self.test_v)
        assert(int(self.field) == int(-self.test_v))
    def test_long(self):
        self.field.set(-self.test_v)
        assert(long(self.field) == long(-self.test_v))
    def test_float(self):
        self.field.set(-self.test_v)
        assert(float(self.field) == float(-self.test_v))
    def test_coerce(self):
        self.field.set(self.test_v)
        r = coerce(self.field, 5)
        assert(type(r[0]) == type(r[1])
               or isinstance(r[0], type(r[1]))
               or isinstance(r[1], type(r[0])))
        r = coerce(self.field, 5.0)
        assert(type(r[0]) == type(r[1])
               or isinstance(r[0], type(r[1]))
               or isinstance(r[1], type(r[0])))

class TestIntegerField(TestField, TestNumericField):
    def setUp(self):
        super(TestIntegerField, self).setUp()
        self.field = self.gob.integer_field
        self.test_v = 5
        self.test_less = 2

    def test_validate(self):
        self.assertRaises(TypeError, self.field.set, '5')
        self.assertRaises(ValueError, self.field.set, self.field.maximum + 1)
        self.field = 5

    def test_lshift(self):
        self.field.set(123)
        assert(self.field << 3 == 123 << 3)
    def test_rshift(self):
        self.field.set(123)
        assert(self.field >> 3 == 123 >> 3)
    def test_and(self):
        self.field.set(123)
        assert(self.field & 321 == 123 & 321)
    def test_xor(self):
        self.field.set(123)
        assert(self.field ^ 321 == 123 ^ 321)
    def test_or(self):
        self.field.set(123)
        assert(self.field | 321 == 123 | 321)

    def test_rlshift(self):
        self.field.set(3)
        assert(123 << self.field == 123 << 3)
    def test_rrshift(self):
        self.field.set(3)
        assert(123 >> self.field == 123 >> 3)
    def test_rand(self):
        self.field.set(123)
        assert(321 & self.field == 321 & 123)
    def test_rxor(self):
        self.field.set(123)
        assert(321 ^ self.field == 321 ^ 123)
    def test_ror(self):
        self.field.set(123)
        assert(321 | self.field == 321 | 123)

    def test_ilshift(self):
        self.field.set(123)
        self.field <<= 3
        assert(self.field == 123 << 3)
    def test_irshift(self):
        self.field.set(123)
        self.field >>= 3
        assert(self.field == 123 >> 3)
    def test_iand(self):
        self.field.set(123)
        self.field &= 321
        assert(self.field == 123 & 321)
    def test_ixor(self):
        self.field.set(123)
        self.field ^= 321
        assert(self.field == 123 ^ 321)
    def test_ior(self):
        self.field.set(123)
        self.field |= 321
        assert(self.field == 123 | 321)

    def test_invert(self):
        self.field.set(123)
        assert(~self.field == ~123)
    def test_oct(self):
        self.field.set(123)
        assert(oct(self.field) == '0173')
    def test_hex(self):
        self.field.set(123)
        assert(hex(self.field) == '0x7b')

class TestIncrementingField(TestIntegerField):
    def setUp(self):
        super(TestIncrementingField, self).setUp()
        self.field = self.gob.incrementing_field
        self.test_v = 5
        self.test_less = 2

class TestRealField(TestField, TestNumericField):
    def setUp(self):
        super(TestRealField, self).setUp()
        self.field = self.gob.real_field
        self.test_v = 3.1416
        self.test_less = 1.618

    def test_validate(self):
        self.assertRaises(TypeError, self.field.set, 'abc')
        self.field.set(1.618)

class TestEnumField(TestStringField):
    def setUp(self):
        super(TestEnumField, self).setUp()
        self.field = self.gob.enum_field
        self.test_v = 'test2'
        self.test_less = 'test1'

    def test_validate(self):
        self.assertRaises(TypeError, self.field.set, 234)
        self.assertRaises(ValueError, self.field.set, 'abc')
        self.field.set('test2')

    def test_imul(self):
        pass
    def test_iadd(self):
        pass
    def test_encode(self):
        pass
    def test_decode(self):
        pass

class TestUUIDField(TestStringField):
    def setUp(self):
        super(TestUUIDField, self).setUp()
        self.field = self.gob.uuid_field
        self.test_v = '2ef6eaf0-d2d0-11e1-9b23-0800200c9a66'
        self.test_less = '1da2e1f0-d2d0-11e1-9b23-0800200c9a66'

    def test_validate(self):
        self.assertRaises(TypeError, self.field.set, 234)
        self.assertRaises(ValueError, self.field.set, 'abc')
        self.field.set('2ef6eaf0-d2d0-11e1-9b23-0800200c9a66')

    def test_imul(self):
        pass
    def test_iadd(self):
        pass
    def test_encode(self):
        pass
    def test_decode(self):
        pass

class TestMultiField(object):
    def setUp(self):
        pass

    def test_iter(self):
        tv_copy = list(self.test_v)
        self.field.set(self.test_v)
        for x in self.field:
            assert(x in tv_copy)
            tv_copy.remove(x)
        assert(len(tv_copy) == 0)

    def test_len(self):
        self.field.set(self.test_v)
        assert(len(self.field) == len(self.test_v))

    def test_contains(self):
        tv_copy = list(self.test_v)
        self.field.set(self.test_v)
        assert(tv_copy[0] in self.field)

class TestListField(TestField, TestMultiField):
    def setUp(self):
        super(TestListField, self).setUp()
        self.field = self.gob.list_field
        self.test_v = [3, 7]
        self.test_less = [2, 6]

    def test_validate(self):
        self.assertRaises(TypeError, self.field.set, 234)
        self.assertRaises(TypeError, self.field.set, ['abc'])
        self.field.set([3, 7])

    def test_setitem(self):
        self.field.set([3, 7])
        self.field[0] = 2
        assert(self.field == [2, 7])

    def test_delitem(self):
        self.field.set([3, 5, 7])
        del self.field[1]
        assert(self.field == [3, 7])

    def test_iadd(self):
        self.field.set([3, 5])
        self.field += [7]
        assert(self.field == [3, 5, 7])

    def test_imul(self):
        self.field.set([3, 5])
        self.field *= 2
        assert(self.field == [3, 5, 3, 5])

    def test_reverse(self):
        self.field.set([3, 5, 7])
        self.field.reverse()
        assert(self.field == [7, 5, 3])

    def test_sort(self):
        self.field.set([7, 2, 5, 3])
        self.field.sort()
        assert(self.field == [2, 3, 5, 7])

    def test_extend(self):
        self.field.set([3, 5, 7])
        self.field.extend([8, 9])
        assert(self.field == [3, 5, 7, 8, 9])

    def test_insert(self):
        self.field.set([3, 5, 7])
        self.field.insert(2, 6)
        assert(self.field == [3, 5, 6, 7])

    def test_pop(self):
        self.field.set([3, 5, 7])
        r = self.field.pop()
        assert(r == 7)
        assert(self.field == [3, 5])

    def test_remove(self):
        self.field.set([3, 5, 7])
        self.field.remove(5)
        assert(self.field == [3, 7])

    def test_append(self):
        self.field.set([3, 5])
        self.field.append(7)
        assert(self.field == [3, 5, 7])

    def test_getitem(self):
        self.field.set([3, 5, 7])
        assert(self.field[1] == 5)

    def test_add(self):
        self.field.set([3, 5])
        assert(self.field + [7, 9] == [3, 5, 7, 9])

    def test_radd(self):
        self.field.set([3, 5])
        assert([7, 9] + self.field == [7, 9, 3, 5])

    def test_mul(self):
        self.field.set([3, 5])
        assert(self.field * 2 == [3, 5, 3, 5])

    def test_rmul(self):
        self.field.set([3, 5])
        assert(2 * self.field == [3, 5, 3, 5])

class TestSetField(TestField, TestMultiField):
    def setUp(self):
        super(TestSetField, self).setUp()
        self.field = self.gob.set_field
        self.test_v = set([3, 7])
        # set comparisons are actually tests for superset/subset
        self.test_less = set([3])

    def test_validate(self):
        self.assertRaises(TypeError, self.field.set, 234)
        self.assertRaises(TypeError, self.field.set, set(['abc']))
        self.field.set(set([3, 7]))

    def test_update(self):
        self.field.set(set([3, 7]))
        self.field.update(set([4, 5, 1]))
        assert(self.field == set([1, 3, 4, 5, 7]))

    def test_ior(self):
        self.field.set(set([3, 7]))
        self.field |= set([5])
        assert(self.field == set([3, 5, 7]))

    def test_intersection_update(self):
        self.field.set(set([3, 7]))
        self.field.intersection_update(set([3, 4]))
        assert(self.field == set([3]))

    def test_iand(self):
        self.field.set(set([3, 7]))
        self.field &= set([3, 4])
        assert(self.field == set([3]))

    def test_difference_update(self):
        self.field.set(set([1, 3, 7]))
        self.field.difference_update(set([1, 3]))
        assert(self.field == set([7]))

    def test_isub(self):
        self.field.set(set([1, 3, 7]))
        self.field -= set([1, 3])
        assert(self.field == set([7]))

    def test_symmetric_difference_update(self):
        self.field.set(set([1, 3, 7]))
        self.field.symmetric_difference_update(set([1, 3, 4]))
        assert(self.field == set([4, 7]))

    def test_ixor(self):
        self.field.set(set([1, 3, 7]))
        self.field ^= set([1, 3, 4])
        assert(self.field == set([4, 7]))

    def test_add(self):
        self.field.set(set([3, 7]))
        self.field.add(1)
        assert(self.field == set([1, 3, 7]))

    def test_remove(self):
        self.field.set(set([1, 3, 7]))
        self.field.remove(3)
        assert(self.field == set([1, 7]))

    def test_discard(self):
        self.field.set(set([1, 3, 7]))
        self.field.discard(3)
        assert(self.field == set([1, 7]))
        self.field.discard(4)
        assert(self.field == set([1, 7]))

    def test_pop(self):
        self.field.set(set([1, 3, 7]))
        r = self.field.pop()
        assert(r in (1, 3, 7))
        assert(len(self.field) == 2)

    def test_clear(self):
        self.field.set(set([1, 3, 7]))
        self.field.clear()
        assert(self.field == set([]))

    def test_or(self):
        self.field.set(set([3, 7]))
        assert(self.field | set([5]) == set([3, 5, 7]))

    def test_and(self):
        self.field.set(set([3, 5, 7]))
        assert(self.field & set([3, 4, 7]) == set([3, 7]))

    def test_sub(self):
        self.field.set(set([3, 5, 7]))
        assert(self.field - set([2, 5]) == set([3, 7]))

    def test_xor(self):
        self.field.set(set([3, 5, 7]))
        assert(self.field ^ set([2, 5]) == set([2, 3, 7]))

    def test_copy(self):
        self.field.set(set([3, 5, 7]))
        r = self.field.copy()
        assert(r is not self.field)
        assert(self.field == r)

class TestForeignObject(TestField, TestGob):
    def setUp(self):
        super(TestForeignObject, self).setUp()
        self.gob.save()
        self.gob2.save()
        self.sc.commit()
        self.field = self.gob2.parent
        self.orig_gob = self.gob
        self.gob = self.gob2.parent

    def tearDown(self):
        self.orig_gob.remove()
        self.gob2.remove()
        self.sc.commit()

    def test_save(self):
        self.gob.string_field = 'changed example string'
        assert(self.gob.value.dirty)
        self.gob.save()
        self.sc.commit()
        assert(not self.gob.value.dirty)
        self.gob2.parent.forget()
        gotten_gob = self.sc.gobtests.get(self.gob_key)
        assert(gotten_gob.string_field == 'changed example string')
        self.gob = self.gob2.parent

    def test_remove(self):
        self.gob.remove()
        self.sc.commit()
        try:
            self.assertRaises(gobpersist.exception.NotFound,
                              self.sc.gobtests.get, self.gob_key)
        finally:
            self.orig_gob.save()
            self.sc.commit()

    # override useless tests
    def test_set(self):
        pass
    def test_null(self):
        pass
    def test_revert(self):
        pass

    # fixme: redefine these
    def test_comparison(self):
        pass
    def test_hash(self):
        pass
    def test_forget(self):
        pass

class TestForeignCollection(TestField):
    def setUp(self):
        super(TestForeignCollection, self).setUp()
        self.gob.save()
        self.gob2.save()
        self.sc.commit()
        self.field = self.gob.children

    def tearDown(self):
        self.gob.remove()
        self.gob2.remove()
        self.sc.commit()

    # override useless tests
    def test_set(self):
        pass
    def test_null(self):
        pass

    # fixme: redefine these
    def test_comparison(self):
        pass
    def test_hash(self):
        pass
    def test_forget(self):
        pass

    def test_list(self):
        r = self.field.list()
        assert(len(r) == 1)
        assert(r[0].primary_key == self.gob2_key)
        r = self.field.list(integer_field=97, string_field='example string 2')
        assert(len(r) == 1)
        assert(r[0].primary_key == self.gob2_key)
        r = self.field.list(integer_field=101)
        assert(len(r) == 0)

    def test_get(self):
        r = self.field.get(self.gob2_key)
        assert(r.primary_key == self.gob2_key)

    def test_add(self):
        self.gob2.remove()
        self.sc.commit()
        self.field.add(self.gob2)
        self.sc.commit()
        r = self.field.get(self.gob2_key)
        assert(r.primary_key == self.gob2_key)

    def test_update(self):
        self.gob2.string_field = 'changed example string 2'
        self.field.update(self.gob2)
        self.sc.commit()
        r = self.field.get(self.gob2_key)
        assert(r.string_field == 'changed example string 2'
               and not r.string_field.dirty)

    def test_remove(self):
        self.field.remove(self.gob2)
        self.sc.commit()
        try:
            self.assertRaises(gobpersist.exception.NotFound,
                              self.field.get, self.gob2_key)
        finally:
            self.field.add(self.gob2)
            self.sc.commit()

if __name__ == '__main__':
    # import cProfile
    # cProfile.run('unittest.main()', sort='cumulative')
   unittest.main()
