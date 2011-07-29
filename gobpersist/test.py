#!/usr/bin/env python
if __name__ == '__main__':
    import os.path
    import sys

    libdir = os.path.join(os.path.dirname(__file__), '..')
    sys.path.insert(0, libdir)


import unittest
from gobpersist import gob
from gobpersist import field
from gobpersist import schema
from gobpersist import session
from gobpersist.backends import memcached
import datetime
import uuid

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
        def store_in_root(self):
            return self.parent_key == None

        def store_in_root_changed(self):
            return self.parent_key.has_value \
                and self.parent_key.persisted_value != self.parent_key.value

        my_key = field.UUIDField(primary_key=True)
        parent_key = field.UUIDField(null=True)

        parent = field.ForeignObject(foreign_class='self', local_key='parent_key', foreign_key='primary_key', virtual=True)
        children = field.ForeignCollection(foreign_class='self', local_key='primary_key', foreign_key='parent_key', name='children')

        keys = [('self', parent_key, 'children')]
        unique_keys = [('gobtests_by_timestamp', timestamp_field)]

        pass

    return GobTest

def get_gob_schema():
    class TestSchema(schema.Schema):
        gobtests = get_gob_class()
    return TestSchema

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
        s = session.Session(backend=memcached.MemcachedBackend())

    def test_schema_creation(self):
        sc = get_gob_schema()(session=session.Session(backend=memcached.MemcachedBackend()))

    def test_gob_creation(self):
        sc_class = get_gob_schema()
        sc = sc_class(session=session.Session(backend=memcached.MemcachedBackend()))
        gob = sc_class.gobtests(sc)

class TestWithSchema(unittest.TestCase):
    def setUp(self):
        super(TestWithSchema, self).setUp()
        self.sc_class = get_gob_schema()
        self.sc = self.sc_class(session=session.Session(backend=memcached.MemcachedBackend()))

class TestWithGob(TestWithSchema):
    def setUp(self):
        super(TestWithGob, self).setUp()
        self.gob = self.sc_class.gobtests(self.sc)
        self.gob.boolean_field = True
        self.gob.datetime_field = datetime.datetime.utcnow()
        self.gob.string_field = 'example_string'
        self.gob.integer_field = 25
        self.gob.real_field = 137542.5
        self.gob.enum_field = 'test2'
        self.gob.uuid_field = str(uuid.uuid4())
        self.gob.primary_key = str(uuid.uuid4())
        self.gob.parent_key = None
        
class TestList(TestWithGob):
    def test_list(self):
        self.gob.save()
        self.sc.commit()
        self.sc.gobtests.list()
        self.gob.remove()
        self.sc.commit()
        

class TestAdd(TestWithGob):
    def test_persist(self):
        self.gob.save()
        self.sc.commit()
        self.gob.remove()
        self.sc.commit()

    def test_update(self):
        self.gob.save()
        self.sc.commit()
        self.gob.string_field = "new string!"
        self.gob.save()
        self.sc.commit()
        self.gob.remove()
        self.sc.commit()

if __name__ == '__main__':
    unittest.main()