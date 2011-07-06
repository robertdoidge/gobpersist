# The import dragons will keep you from doing something more obvious.
from gobpersist import field

def field_key(key):
    return '_Field__' + key

class GobMetaclass(type):
    """Metaclass for the Gob class."""
    def __init__(cls, *args, **kwargs):
        cls.reload_class()
        super(GobMetaclass, cls).__init__(*args, **kwargs)

class Gob(object):
    """Abstract type to represent a persistent object."""
    __metaclass__ = GobMetaclass

    @classmethod
    def reload_class(cls):
        primary_key = None
        for key, value in cls.__dict__.iteritems():
            if isinstance(value, field.Field):
                value._key = field_key(key)
                if value.name is None:
                    value.name = key
                if value.primary_key:
                    primary_key = value
            elif isinstance(value, field.ForeignCollection):
                value._key = field_key(key)
                if value.foreign_class == 'self':
                    value.foreign_class = cls
        cls.primary_key = primary_key
        if 'collection_name' not in cls.__dict__:
            cls.collection_name = cls.__name__.lower() + 's'

    def __init__(self, session=None, _incoming_data=False, **kwdict):
        self.persisted = _incoming_data
        self.session = session
        self.dirty = False
        for key in dir(self.__class__):
            value = getattr(self.__class__, key)
            if isinstance(value, field.Field):
                value = value.clone()
                value.instance = self
                self.__dict__[value._key] = value
                if value.primary_key:
                    self.__dict__['primary_key'] = value

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

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, ', '.join(set(["%s=%s" % (value.name, repr(value)) for value in filter(lambda x: isinstance(x, field.Field), self.__dict__.values())])))
