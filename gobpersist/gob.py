from __future__ import absolute_import
from . import field

def field_key(key):
    return '_Field__' + key

class GobMetaclass(type):
    """Metaclass for the Gob class."""
    def __init__(cls, *args, **kwargs):
        for key, value in cls.__dict__.iteritems():
            if isinstance(value, field.Field):
                value._key = field_key(key)
                if value.name is None:
                    value.name = key
        if 'collection_name' not in cls.__dict__:
            cls.collection_name = cls.__name__.lower() + 's'
        super(GobMetaclass, cls).__init__(*args, **kwargs)

class Gob(object):
    """Abstract type to represent a persistent object."""
    __metaclass__ = GobMetaclass

    def __init__(self, session, _incoming_data=False, **kwdict):
        self.persisted = _incoming_data
        self.session = session
        self.dirty = False
        for value in self.__class__.__dict__.itervalues():
            if isinstance(value, field.Field):
                value = value.clone()
                value.instance = self
                self.__dict__[value._key] = value
                if value.primary_key:
                    self.primary_key = value

        for key, value in kwdict.iteritems():
            f_key = field_key(key)
            if f_key not in self.__dict__:
                raise TypeError("__init__() got an unexpected keyword argument '%s'" % key)
            can_modify=self.__dict__[f_key].modifiable
            if(_incoming_data):
                self.__dict__[f_key].modifiable = True
            self.__dict__[f_key].set(value)
            if(_incoming_data):
                self.__dict__[f_key].modifiable = can_modify
                self.__dict__[f_key].reset_state()

    def save(self):
        if self.persisted:
            self.session.update(self)
        else:
            self.session.add(self)

    def remove(self):
        self.session.remove(self)

    def prepare_persist(self):
        for value in self.__dict__.itervalues():
            if isinstance(value, field.Field):
                value.prepare_persist()

    def mark_persisted(self):
        self.persisted = True
        for value in self.__dict__.itervalues():
            if isinstance(value, field.Field):
                value.mark_clean()

    def path(self):
        return (self.__class__, self.primary_key)
