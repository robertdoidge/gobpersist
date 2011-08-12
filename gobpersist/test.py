#!/usr/bin/env python
if __name__ == '__main__':
    import os.path
    import sys

    libdir = os.path.join(os.path.dirname(__file__), '..')
    sys.path.insert(0, libdir)


import unittest
import datetime
import iso8601
import uuid

import gserialize

from gobpersist import gob
from gobpersist import field
from gobpersist import schema
from gobpersist import session
from gobpersist import storage
from gobpersist.backends import memcached
from gobpersist.backends import gobkvquerent
from gobpersist.backends import cache

def get_gob_class():
    class GobTest(gob.Gob):
        boolean_field = field.BooleanField()
        datetime_field = field.DateTimeField()
        timestamp_field = field.TimestampField(unique=True)
        string_field = field.StringField()
        integer_field = field.IntegerField()
        incrementing_field = field.IncrementingField()
        real_field = field.RealField()
        enum_field = field.EnumField(choices=('test1','test2'))
        uuid_field = field.UUIDField()
        # list_field = field.ListField(element=field.IntegerField())
        # set_field = field.SetField(element=field.IntegerField())
        def keyset(self):
            if self.parent_key == None:
                return [self.coll_key] + self.keys
            else:
                return self.keys

        my_key = field.UUIDField(primary_key=True)
        parent_key = field.UUIDField(null=True)

        parent = field.ForeignObject(foreign_class='self',
                                     local_field='parent_key',
                                     foreign_field='primary_key')
        children = field.ForeignCollection(foreign_class='self',
                                           local_field='primary_key',
                                           foreign_field='parent_key',
                                           key=('gobtests', my_key,
                                                'children'))

        keys = [('self', parent_key, 'children')]
        unique_keys = [('gobtests_by_timestamp', timestamp_field)]

        pass

    return GobTest

def get_gob_schema():
    class TestSchema(schema.Schema):
        gobtests = get_gob_class()
    return TestSchema

def get_memcached():
    return memcached.MemcachedBackend(expiry=60,
                                      serializer=gserialize.JSONSerializer())

class Initialization(unittest.TestCase):
    def test_gob_definition(self):
        gob_class = get_gob_class()
        assert(issubclass(gob_class, gob.Gob))

    def test_gob_reload(self):
        gob_class = get_gob_class()
        gob_class.new_field = field.IntegerField()
        gob_class.reload_class()

    def test_schema_definition(self):
        gob_schema = get_gob_schema()

    def test_session_creation(self):
        s = session.Session(backend=get_memcached())

    def test_schema_creation(self):
        sc = get_gob_schema()(session=session.Session(backend=get_memcached()))

    def test_gob_creation(self):
        sc_class = get_gob_schema()
        sc = sc_class(session=session.Session(backend=get_memcached()))
        gob = sc_class.gobtests(sc)
        gob2 = sc_class.gobtests(sc)

class TestWithSchema(unittest.TestCase):
    def setUp(self):
        super(TestWithSchema, self).setUp()
        self.sc_class = get_gob_schema()
        self.sc = self.sc_class(session=session.Session(backend=get_memcached()))

#gobpersist/schema.py
#gobpersist/gob.py

class TestWithGob(TestWithSchema):
    gob_key = str(uuid.uuid4())
    gob2_key = str(uuid.uuid4())

    def setUp(self):
        super(TestWithGob, self).setUp()
        self.gob = self.sc_class.gobtests(self.sc)
        self.gob.boolean_field.set(True)
        self.gob.datetime_field = datetime.datetime.utcnow()
        self.gob.string_field = 'example_string'
        self.gob.integer_field = 25
        self.gob.real_field = 137542.5
        self.gob.enum_field = 'test2'
        self.gob.uuid_field = str(uuid.uuid4())
        self.gob.primary_key = self.gob_key
        self.gob.parent_key = None

        self.gob2 = self.sc_class.gobtests(self.sc)
        self.gob2.boolean_field.set(True)
        self.gob2.datetime_field = datetime.datetime.utcnow()
        self.gob2.string_field = 'example_string'
        self.gob2.integer_field = 30
        self.gob2.real_field = 12345.5
        self.gob2.enum_field= 'test2'
        self.gob2.primary_key = self.gob2_key
    
    def test_keyset(self):
        keys = self.gob.keyset()
        print '\ntest_keyset '
        print keys
        print '\n'

    def test_uniquekeyset(self):
        unique_keys = self.gob.unique_keyset()
        print '\ntest_uniquekeyset '
        print unique_keys
        print '\n'

    def test_get(self):
        print '\nmaintest_get'
        self.gob.save()
        self.sc.commit()
        self.sc.gobtests.get(self.gob_key)
        self.gob.remove()
        self.sc.commit()
        print '\n'
        
    def test_list(self):
        print '\nmaintest_list'
        self.gob.save()
        self.sc.commit()
        self.sc.gobtests.list(name='gob')
        self.gob2.remove()
        self.sc.commit()
        print '\n'

    def test_update(self):
        print '\nmaintest_update'
        self.gob.save()
        self.sc.commit()
        self.gob.integer_field = 30
        self.sc.update(self.gob)
        self.sc.commit()
        self.gob.remove()
        self.sc.commit()
        print '\n'

    def test_rollback(self):
        self.gob.save()
        print'\nmaintest_rollback'
        print self.sc.operations
        self.gob.remove()
        self.sc.rollback()
        print self.sc.operations
        print '\n'

    def test_prepare_add(self):
        self.gob.prepare_add()

    def test_prepare_update(self):
        self.gob.prepare_update()

    def test_revert(self):
        print '\nmaintest_revert'
        print self.gob.revert()
        print self.gob
        print '\n'

    def test_markpersisted(self):
        self.gob.mark_persisted()

    def test__repr__(self):
        print '\nsession.Session.__repr__'
        print self.gob.__repr__()
        print '\n'

    

#gobpersist/schema.py

    def test_collection_for_a_key(self):
        print '\nSchema.collection_for_a_key'
        schema_coll_key = str(uuid.uuid4())
        schema_coll = self.sc.collection_for_key(schema_coll_key)
        print schema_coll
        print '\n'

#gobpersist/session.py
    
    def test_gob_to_mygob(self):
        print '\nsession.GobTranslator.gob_to_mygob'
        print self.gob
        print self.sc.gob_to_mygob(self.gob)
        print '\n'

    def test_query_to_myquery(self):
        self.gob2.save()
        self.sc.commit()
        print '\nsession.GobTranslator.query_to_myquery'
        print self.sc.query_to_myquery(field.IntegerField(), {'integer_field':('eq', 30), 'integer_field':('eq', 15)} )
        print '\n'
        self.gob2.remove()
        self.sc.commit()

    def test_key_to_mykey(self):
        uuid_str = str(uuid.uuid4())
        new_key = ('gobtests', uuid_str)
        print'\nsession.GobTranslator.key_to_mykey'
        print self.sc.key_to_mykey(new_key)
        print '\n'

    def test_retrieve_to_myretrieve(self):
        print '\nsession.GobTranslator.retrieve_to_myretrieve'
        print self.sc.retrieve_to_myretrieve(self.gob,  ['integer_field'])
        print self.sc.retrieve_to_myretrieve(self.gob, ['my_key'])
        print '\n'

    def test_query(self):
        self.gob2.save()
        self.sc.commit()
        print '\nsession.GobTranslator.query'
        print self.sc.query(query=('eq', 30))
        print '\n'
        self.gob2.remove()
        self.sc.commit()

#gobpersist/backends/memcached.py

    def test_dokvmultiquery(self):
        print '\nbackends.memcached.dokvmultiquery'
        self.gob.save()
        self.gob2.save()
        self.sc.commit()
        key_list = [('gobtests', self.gob_key), ('gobtests', self.gob2_key)]
        self.sc.session.backend.do_kv_multi_query(self.sc_class.gobtests, key_list)
        self.gob.remove()
        self.gob2.remove()
        self.sc.commit()
        print '\n'
        
    def test_dokvquery(self):
        print '\nbackends.memcached.dokvquery'
        self.gob.save()
        self.sc.commit()
        key = ('gobtests', self.gob_key)
        self.sc.session.backend.do_kv_query(self.sc_class.gobtests, key)
        self.gob.remove()
        self.sc.commit()
        print '\n'

    def test_kvquery(self):
        print '\nbackends.memcached.test_kvquery'
        key = ('gobtests', self.gob_key)
        self.sc.session.backend.kv_query(self.sc_class.gobtests, key=key)
        print '\n'

    def test_acquire_and_release_locks(self):
        print '\nbackends.memcached.aquire_locks, release_locks'
        locks = set()
        locks.add('_lock' + '.' + '.'.join(self.gob_key))
        self.sc.session.backend.acquire_locks(locks)
        self.sc.session.backend.release_locks(locks)
        print '\n'
        
    def test_keytomykey(self):
        print '\nbackends.memcached.key_to_mykey'
        key = ('gobtests', self.gob_key)
        print key
        gob_key = self.sc.session.backend.key_to_mykey(key)
        print gob_key
        print '\n'
    
    def test_commit(self):
        print '\nbackends.memcached.commit'
        self.gob.save()
        self.sc.commit()
        self.gob.remove()
        self.sc.commit()
        print '\n\n'

#gobpersist/backends/gobkvquerent.py
    
    def test_getvalue_gobkv(self):
        new_sc_class = get_gob_schema()
        new_sc = new_sc_class(session=session.Session(backend=gobkvquerent.GobKVQuerent()))
        print'\ntest_getvalue_gobkv'
        print new_sc.session.backend._get_value(self.gob, self.gob.integer_field)
        print '\n'

    def test_apply_operator_gobkv(self):
        new_sc_class = get_gob_schema()
        new_sc = new_sc_class(session=session.Session(backend=gobkvquerent.GobKVQuerent()))
        print '\ntest_applyoperator_gobkv'
        print new_sc.session.backend._apply_operator(self.gob, 'eq', self.gob.integer_field, self.gob2.integer_field) 
        print '\n'
        
    def test_executequery_gobkv(self):
        new_sc_class = get_gob_schema()
        new_sc = new_sc_class(session=session.Session(backend=gobkvquerent.GobKVQuerent()))
        print '\ntest_executequery_gobkv'
        print new_sc.session.backend._execute_query(self.gob,{'eq': 30, 'ne': 'garbage string'} )
        print '\n'

    def test_query_gobkv(self):
        new_sc_class = get_gob_schema()
        new_sc = new_sc_class(session=session.Session(backend=gobkvquerent.GobKVQuerent()))
        print '\ntest_query_gobkv'
        print new_sc.session.backend.query(new_sc.gobtests, query={'eq': 30})
        print '\n'

#gobpersist/backends/cache.py
#not sure if we can test this yet
    def test_query_cache(self):
        new_sc_class = get_gob_schema()
        new_sc = new_sc_class(session=session.Session(backend=cache.Cache()))
        print '\ntest_query_cache'
        print new_sc.session.backend.query(new_sc.gobtests, query={'eq': 30})

class FieldGroup(object):
    def __init__(self):
        self.boolean_field = field.BooleanField()
        self.datetime_field = field.DateTimeField()
        self.string_field = field.StringField()
        self.integer_field = field.IntegerField()
        self.incrementing_field = field.IncrementingField()
        self.real_field = field.RealField()
        self.enum_field = field.EnumField(choices=('test1', 'test2'))
        self.uuid_field = field.UUIDField()

class FieldWorkout(unittest.TestCase):    
    def setUp(self):
        self.tstfields = FieldGroup()
    
    def test_prepareadd(self):
        self.tstfields.integer_field.has_value = False
        self.tstfields.integer_field.default = 2312

        self.tstfields.integer_field.value = 1234
        self.tstfields.integer_field.prepare_add()

        self.assertEqual(self.tstfields.integer_field.value, 2312)

    def test_prepareupdate(self):
        self.tstfields.integer_field.value = 1234
        self.tstfields.integer_field.default_update = field.IntegerField.set(self.tstfields.integer_field, 12)
        self.tstfields.integer_field.dirty = False

        self.tstfields.integer_field.prepare_update()

        self.assertEqual(self.tstfields.integer_field.value, 12)

    def test_tripset(self):
        self.tstfields.boolean_field = field.BooleanField()
        self.tstfields.boolean_field.trip_set()

        self.assertEqual(self.tstfields.boolean_field.dirty, True)
        self.assertEqual(self.tstfields.boolean_field.has_value, True)

    def test_resetstate(self):
        self.tstfields.string_field.immutable = True
        self.tstfields.string_field.dirty = True
        self.tstfields.string_field.reset_state()

        self.assertEqual(self.tstfields.string_field.immutable, False)
        self.assertEqual(self.tstfields.string_field.dirty, False)
        
    def test_markpersisted(self):
        field_value = 1234
        self.tstfields.integer_field = field.IntegerField()
        self.tstfields.integer_field.dirty = True
        self.tstfields.integer_field.has_value = False
        
        self.tstfields.integer_field.mark_persisted()
        self.assertEqual(self.tstfields.integer_field.has_persisted_value, False)
        
        self.tstfields.integer_field.set(field_value)
        self.tstfields.integer_field.has_value = True

        self.tstfields.integer_field.mark_persisted()

        self.assertEqual(self.tstfields.integer_field.has_persisted_value, True)
        self.assertEqual(self.tstfields.integer_field.persisted_value, field_value)

    def test_revert(self):
        field_value = 1234
        self.tstfields.integer_field.persisted_value = field_value
        self.tstfields.integer_field.set(4567)
        self.tstfields.integer_field.has_persisted_value = True
        self.tstfields.dirty = True

        self.tstfields.integer_field.revert()

        self.assertEqual(self.tstfields.integer_field.has_value, True)
        self.assertEqual(self.tstfields.integer_field.value, field_value)
        self.assertEqual(self.tstfields.integer_field.dirty, False)
    
    def test_sets(self):
        self.tstfields.integer_field.set(1234)
        self.assertEqual(self.tstfields.integer_field.value, 1234)

        self.tstfields.string_field.set('outta')
        self.assertEqual(self.tstfields.string_field.value, 'outta')
        
    def test_validate(self):
        self.assertRaises(ValueError, self.tstfields.boolean_field.validate, None)

    def test_clone(self):
        self.tstfields.string_field.set("outta control string")
        new_field = field.Field.clone(self.tstfields.string_field)
        self.assertEqual(self.tstfields.string_field, new_field)

        self.tstfields.integer_field.set(1234)
        new_field = field.Field.clone(self.tstfields.integer_field)
        self.assertEqual(self.tstfields.integer_field, new_field) 

    def test_set__(self):
        self.tstfields.integer_field.set(1234)
        self.tstfields.integer_field.__set__(self.tstfields.integer_field, 5678)

    def test_get__(self):
        self.tstfields.integer_field.set(5678)
        self.tstfields.integer_field.__get__(self.tstfields.integer_field, self.tstfields)

    def test_delete__(self):
        self.tstfields.integer_field.set(1234)
        self.tstfields.__delete__(self.tstfields.integer_field)

class BooleanFieldWorkout(unittest.TestCase):
    def setUp(self):
        self.boolean_field = field.BooleanField()

    def test_validate(self):
        self.assertRaises(TypeError, self.boolean_field.validate, 1234)

class DateTimeFieldWorkout(unittest.TestCase):
    def setUp(self):
        self.datetime_field = field.DateTimeField()
    
    def test_set(self):
        datetime_value = "2007-06-20T12:34:40Z"
        self.datetime_field._set(datetime_value)
        self.assertEqual(self.datetime_field.value, iso8601.parse_datetime(datetime_value))

    def test_validate(self):
        self.assertRaises(TypeError, self.datetime_field.validate, True)
        self.assertRaises(ValueError, self.datetime_field.validate, " 'ello suh ")
        
        self.datetime_field.validate("2007-06-20T12:34:40Z")

class TimestampFieldWorkout(unittest.TestCase):
    def setUp(self):
        self.timestamp_field = field.TimestampField()

    def test_field(self):
        compare_date = datetime.datetime(2007, 4, 3)
        self.assertEqual(type(self.timestamp_field.default()), type(compare_date))
        
class StringFieldWorkout(unittest.TestCase):
    def setUp(self):
        self.string_field = field.StringField()

    def test_set(self):
        self.string_field.value = 'this'
        self.value_encoded = 'somethin'
        self.value_decoded = 'sorta'
        self.string_field._set(None)

        self.assertEqual(self.string_field.value, None)
        self.assertEqual(self.string_field.value_encoded, None)
        self.assertEqual(self.string_field.value_decoded, None)

        self.string_field._set(unicode('stuff'))
        self.assertEqual(self.string_field.value, unicode('stuff'))
        self.assertEqual(self.string_field.value_decoded, self.string_field.value)
        self.assertEqual(self.string_field.value_encoded, unicode('stuff').encode('utf-8'))

        self.string_field._set('stuff')
        self.assertEqual(self.string_field.value, 'stuff')
        self.assertEqual(self.string_field.value_encoded, 'stuff')
        self.assertEqual(self.string_field.value_decoded, None)

        self.string_field.encoding = 'utf-8'

        self.string_field._set(unicode('stuff'))
        self.assertEqual(self.string_field.value, unicode('stuff'))
        self.assertEqual(self.string_field.value_decoded, self.string_field.value)
        self.assertEqual(self.string_field.value_encoded, unicode('stuff').encode('utf-8'))

        self.string_field._set('stuff')
        self.assertEqual(self.string_field.value, 'stuff')
        self.assertEqual(self.string_field.value_encoded, 'stuff')
        self.assertEqual(self.string_field.value_decoded, 'stuff'.decode('utf-8'))

    def test_validate(self):
        self.assertRaises(ValueError, self.string_field.validate, None)
        self.assertRaises(TypeError, self.string_field.validate, 1234)
        
        self.string_field.max_length = 5
        self.string_field.allow_empty = False
        self.assertRaises(ValueError, self.string_field.validate, 'sixtyone')
        self.assertRaises(ValueError, self.string_field.validate, '')
        
    def test_unicode__(self):
        self.value_decoded = None
        self.string_field.set('unicode string')
        self.string_field.__unicode__()

    def test_decode(self):
        pass
        
class IntegerFieldWorkout(unittest.TestCase):
    def setUp(self):
        self.integer_field = field.IntegerField()
    
    def test_init(self):
        precision_list = [8, 16, 32, 64]
        
        for count in precision_list:
            field.IntegerField(precision=count)
        
        self.assertRaises(ValueError, field.IntegerField, precision=65)

    def test_validate(self):
        self.assertRaises(TypeError, self.integer_field.validate, uuid.uuid4() )
        self.assertRaises(TypeError, self.integer_field.validate, 'sixteen')

        self.integer_field = field.IntegerField()
        self.integer_field.maximum = 6
        self.integer_field.minimum = 3
        
        self.assertRaises(ValueError, self.integer_field.validate, 7)
        self.assertRaises(ValueError, self.integer_field.validate, 2)

class IncrementingFieldWorkout(unittest.TestCase):
    "Come back to this one, please."
    def setUp(self):
        self.inc_field = field.IncrementingField()

    def test_incrementing(self):
        it_list = [0, 0, 0, 0]
        self.inc_field.set(1)

class RealFieldWorkout(unittest.TestCase):
    def setUp(self):
        self.real_field = field.RealField()

    def test_init(self):
        it_list = ['half', 'single', 'double', 'quad']
        
        for count in it_list:
            self.real_field = field.RealField(precision = count)
            self.assertEqual(self.real_field.precision, count)

    def test_validate(self):
        self.assertRaises(TypeError, self.real_field.set, 1234)
        self.assertRaises(TypeError, self.real_field.set, False)
    
class EnumFieldWorkout(unittest.TestCase):
    def setUp(self):
        self.enum_field = field.EnumField('case1', 'case2')
        
    def test_validate(self):
        self.enum_field.validate('case1')
        self.assertRaises(ValueError, self.enum_field.validate, 'someshit')

class UUIDFieldWorkout(unittest.TestCase):
    def setUp(self):
        self.uuid_field = field.UUIDField()

    def test_init(self):
        self.assertEqual(self.uuid_field.encoding, 'us-ascii')

    def test_validate(self):
        test_uuid =str(uuid.uuid4())
        self.uuid_field.set(test_uuid)
        self.assertRaises(ValueError, self.uuid_field.set, 'randomstring')
        self.assertRaises(ValueError, self.uuid_field.set, uuid.uuid4())

class MultiFieldWorkout(unittest.TestCase):

    def setUp(self):
        self.multi_field = field.MultiField(field.IntegerField())
        self.field_list = [self.multi_field._element_to_field(0),
        self.multi_field._element_to_field(1),
        self.multi_field._element_to_field(2),
        self.multi_field._element_to_field(3)]


    def test_elementtofield(self):
        self.new_field = self.multi_field._element_to_field(4)
        self.assertEqual(self.new_field.value, 4)

        self.multi_field = field.MultiField(field.StringField())
        self.new_field = self.multi_field._element_to_field('viscous')
        self.assertEqual(self.new_field.value, 'viscous')

    def test_resetstate(self):
        self.multi_field.set(self.field_list)
        self.multi_field.reset_state()
        
        self.assertEqual(self.multi_field.value[0].immutable, False)
        self.assertEqual(self.multi_field.value[0].dirty, False)
        self.assertEqual(self.multi_field.value[1].immutable, False)
        self.assertEqual(self.multi_field.value[1].dirty, False)

class ListFieldWorkout(unittest.TestCase):
    def setUp(self):
        self.list_field = field.ListField(field.IntegerField())
        self.int_list = [0, 1, 2, 3, 4]

    def test_clone(self):
        self.list_field.set(self.int_list)
        new_field = self.list_field.clone()
        self.assertEqual(new_field.value, self.int_list)

        str_list = ['zero', 'one', 'two', 'three', 'four']
        self.list_field = field.ListField(field.StringField())
        self.list_field.set(str_list)
        new_field = self.list_field.clone()
        self.assertEqual(new_field.value, str_list)

    def test_set(self):
        self.list_field.set(self.int_list)
        self.assertEqual(self.int_list, self.list_field.value)
        
    def test_setitem(self):
        self.list_field.set(self.int_list)
        self.list_field.__setitem__(0, 1)
        self.assertEqual(self.list_field.value[0], self.int_list[1])
        self.list_field.__setitem__(2, 1)
        self.assertEqual(self.list_field.value[2], self.int_list[1])

    def test_delitem(self):
        self.list_field.set(self.int_list)
        self.list_field.__delitem__(0)
        self.assertEqual(self.list_field.value[0], self.int_list[1])        

    def test_iadd(self):
        int_list2 = [0, 1, 2, 3, 4]
        self.list_field.set(self.int_list)
        self.list_field.__iadd__(int_list2)

    def test_imul(self):
        self.list_field.set(self.int_list)
        self.list_field.__imul__(3)

    def test_reverse(self):
        self.list_field.set(self.int_list)
        self.list_field.reverse()
        self.assertEqual(self.list_field.value, [4, 3, 2, 1, 0])

    def test_sort(self):
        int_field = [5, 3, 9, 4, 1, 3 , 0, 2, 2, 5, 3]
        self.list_field.set(int_field)
        self.list_field.sort()

        self.assertEqual(self.list_field.value, [0, 1, 2, 2, 3, 3, 3, 4, 5, 5, 9])

    def test_extend(self):
        int_list2 = [5, 6, 7, 8]
        self.list_field.set(self.int_list)
        self.list_field.extend(int_list2)
        self.assertEqual(self.list_field.value, [0, 1, 2, 3, 4, 5, 6, 7, 8])

    def test_insert(self):
        self.list_field.set(self.int_list)
        self.list_field.insert(3, 4)
        self.assertEqual(self.list_field.value[3], 4)
    
    def test_pop(self):
        self.list_field.set(self.int_list)
        self.assertEqual(self.list_field.pop(), 4)

    def test_remove(self):
        self.list_field.set(self.int_list)
        self.list_field.remove(2)
        self.assertEqual(self.list_field.value, [0, 1, 3, 4])

    def test_append(self):
        self.list_field.set(self.int_list)
        self.list_field.append(9)
        self.assertEqual(self.list_field.value, [0, 1, 2, 3, 4, 9])
    
    def test_hash(self):
        self.list_field.set(self.int_list)
        temp_list = field.ListField(field.IntegerField())
        temp_list.set(self.int_list)
        self.assertEqual(type(self.list_field.__hash__), type(temp_list.__hash__))

    def test_getitem(self):
        self.list_field.set(self.int_list)
        self.assertEqual(self.list_field.__getitem__(3), 3)

    def test_add(self):
        self.list_field.set(self.int_list)
        self.assertEqual(self.list_field.__add__([5, 6, 7]), [0, 1, 2, 3, 4, 5, 6, 7])

    def test_radd(self):
        self.list_field.set(self.int_list)
        self.assertEqual(self.list_field.__radd__([5, 6, 7]), [5, 6, 7, 0, 1, 2, 3, 4])

    def test_mul(self):
        self.list_field.set(self.int_list)
        self.assertEqual(self.list_field.__mul__(2), [0, 1, 2, 3, 4, 0, 1, 2, 3, 4])

    def test_rmul(self):
        self.list_field.set(self.int_list)
        self.assertEqual(self.list_field.__rmul__(2), [0, 1, 2, 3, 4, 0, 1, 2, 3, 4])

    def test_reversed(self):
        self.list_field.set(self.int_list)
        reversed_list = []
        iterator_obj = self.list_field.__reversed__()
        for i in iterator_obj:
            reversed_list.add(i)
        self.assertEqual(reversed_list, list(reversed(self.list_field.value)))

class ForeignWorkout(unittest.TestCase):
    def setUp(self):
        self.forin_class = FieldGroup()
        self.new_field = field.Foreign(self.forin_class, self.forin_class.integer_field, self.forin_class.boolean_field )
        
    def test_markpersisted(self):
        self.new_field.mark_persisted()
    
    def test_revert(self):
        self.new_field.revert()

    def test_forget(self):
        self.new_field.forget()

    def test_prepare_add(self):
        self.new_field.prepare_add()

    def test_prepare_update(self):
        self.new_field.prepare_update()

    def test_setattribute(self):
        self.new_field.__setattribute__('value', 1234)
        self.assertEqual(self.new_field._value, 1234)

    def test_fetchvalue(self):
        self.new_field.fetch_value()

class ForeignObjectWorkout(unittest.TestCase):
    def setUp(self):
        pass

    def test_fetchvalue(self):
        pass

def init_memcached():
    return memcached.MemcachedBackend(expiry=60, serializer=gserialize.JSONSerializer())

class SizeFileWorkout(unittest.TestCase):
    def setUp(self):
        self.new_file = schema.File()
    
    def test_read(self):
        disk_file = open('sample.txt', 'r')
        file_like = storage.LimitedFile(self.new_file, disk_file)
        file_like.read(4)

class LimitedFileWorkout(unittest.TestCase):
    def setUp(self):
        self.new_file = schema.File()

    def test_read(self):
        disk_file = open('sample.txt', 'r')
        file_like = storage.SizeFile(self.new_file, disk_file)
        file_like.read(5)

class StorableWorkout(unittest.TestCase):
    def setUp(self):
        self.new_file = schema.File()
        self.fp = open('sample.txt', 'r')

    def test_fileops(self):
        self.new_file.upload(self.fp)
        self.new_file.upload_iter(self.fp)

class MD5Workout(unittest.TestCase):
    def setUp(self):
        self.md5_obj = hashlib.md5()
        fp = open('sample.txt', 'r')
        self.new_md5file = storage.MD5File(self.md5_obj, fp)

    def test_read(self):
        self.new_md5file.read(3)

class MD5StorableWorkout(unittest.TestCase):
    def setUp(self):
        self.fp = open('sample.txt', 'r')
        self.md5_storable = storage.MD5Storable()

    def test_upload(self):
        self.md5_storable.upload(self.fp)

    def test_uploaditer(self):
        self.md5_storable.upload_iter(self.fp)

    def test_download(self):
        self.md5_storable.download()
class SessionWorkout(unittest.TestCase):
    class GobTest(gob.Gob):
        boolean_field = field.BooleanField()
        string_field = field.StringField()
        integer_field = field.IntegerField()
        timestamp_field = field.TimestampField(unique=True)

        boolean_field.set(True)
        string_field.set("outtacontrol")
        integer_field.set(1234)

    def setUp(self):
        self.new_gob = self.GobTest
        self.gob_testkey = str(uuid.uuid4())
        self.new_gob.primary_key = self.gob_testkey
        self.new_session = session.Session(memcached.MemcachedBackend(expiry = 60, serializer = gserialize.JSONSerializer()), storage_engine=session.StorageEngine())
         
    def test_registergob(self):
        self.new_session.register_gob(self.new_gob)
        self.assertEqual(self.new_session.collections[self.new_gob.class_key][self.new_gob.primary_key], self.new_gob)

    def test_add(self):
        self.new_session.add(self.new_gob)
        popped_gob = self.new_session.operations['additions'].pop()
        self.assertEqual(popped_gob.primary_key, self.gob_testkey)
         
    def test_update(self):
        self.new_session.update(self.new_gob)
        popped_gob = self.new_session.operations['updates'].pop()
        self.assertEqual(popped_gob.primary_key, self.gob_testkey)

    def test_remove(self):
        self.new_session.remove(self.new_gob)
        popped_gob = self.new_session.operations['removals'].pop()
        self.assertEqual(popped_gob.primary_key, self.gob_testkey)


    def test_addcollection(self):
        path_key = str(uuid.uuid4())
        new_path = tuple( ('new_collection', path_key) )
        self.new_session.add_collection(new_path)
        popped_path = self.new_session.operations['collection_additions'].pop()
        self.assertEqual(popped_path, new_path)
  
    def test_removecollection(self):
        path_key = str(uuid.uuid4())
        new_path = tuple( ('dead_collection', path_key) )
        self.new_session.remove_collection(new_path)
        popped_path = self.new_session.operations['collection_removals'].pop()
        self.assertEqual(popped_path, new_path)

    def test_query(self):
        pass

    def test_updateobject(self):
        cmp_gob = self.new_gob
        cmp_gob.integer_field = 5678
        cmp_gob.string_field = "waydifferent"
        cmp_gob.boolean_field = False

        self.new_session._update_object(cmp_gob, self.new_gob)
        self.assertEqual(cmp_gob, self.new_gob)
    
    def test_starttransaction(self):
        cmp_operations = {
            'additions': set(),
            'removals': set(),
            'updates': set(),
            'collection_additions': set(),
            'collection_removals': set()
            }
        self.new_session.add(self.new_gob)
        self.new_session.update(self.new_gob)
        self.new_session.remove(self.new_gob)
        self.new_session.start_transaction()
        self.assertEqual(cmp_operations, self.new_session.operations)

    def test_commit(self):
        print '\nsession.Session.commit'
        cmp_gob = self.new_gob
        cmp_gob.integer_field = 5678
        cmp_gob.string_field = "waydifferent"
        cmp_gob.boolean_field = False
        cmp_gob.unique_keys = [('timestamp_key', cmp_gob.timestamp_field)]
        print '\n'

    def test_upload(self):
        fp = open('sample.txt', 'r')
        self.new_session.upload(self.new_gob, fp)
    
    def test_uploaditer(self):
        fp = open('sample.txt', 'r')
        self.new_session.upload_iter(self.new_gob, fp)
    
    def test_download(self):
        print'\nSessionWorkout.test_download'
        print self.new_session.download(self.new_gob)
        print '\n'
        
class BackendWorkout(unittest.TestCase):
    def setUp(self):
        self.new_backend = session.Backend()

    def test_commit(self):
        self.new_backend.commit()

class StorageEngineWorkout(unittest.TestCase):
    class GobTest(gob.Gob):
        boolean_field = field.BooleanField()
        string_field = field.StringField()
        integer_field = field.IntegerField()
        timestamp_field = field.TimestampField(unique=True)

        boolean_field.set(True)
        string_field.set("outtacontrol")
        integer_field.set(1234)

    def setUp(self):
        self.new_gob = self.GobTest
        self.new_storage = session.StorageEngine()
        self.fp = open('sample.txt', 'r')
        
    def test_upload(self):
        self.new_storage.upload(self.new_gob, self.fp)

    def test_download(self):
        self.new_storage.download(self.new_gob, self.fp)

    def test_uploaditer(self):
        self.fps = [self.fp, self.fp, self.fp]
        self.new_storage.download(self.new_gob, self.fps)

    
if __name__ == '__main__':
    unittest.main()
