from __future__ import absolute_import
from . import field

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

    primary_key = None
    """Alias of the primary key for this class.

    Set automatically.
    """

    collection_name = None
    """Name of the collection this class represents.

    The default is the name of the class, lower-cased, plus an 's'.
    Subclasses should set this in order to override the default.
    """

    coll_path = None
    """The path for the collection this class represents.

    Default is (collection_name,).
    """

    @classmethod
    def reload_class(cls):
        """Reload the class as if it was recreated from the metaclass.
        """
        primary_key = None
        for key, value in cls.__dict__.iteritems():
            if key != 'primary_key' and isinstance(value, field.Field):
                value._key = field_key(key)
                value.key = key
                if value.primary_key:
                    if primary_key:
                        raise ValueError("More than one primary key defined" \
                                             " for class '%s'" % cls.__name__)
                    primary_key = value
                if value.name is None:
                    value.name = key
                if isinstance(value, field.Foreign):
                    if value.foreign_class == 'self':
                        value.foreign_class = cls
        cls.primary_key = primary_key
        if 'collection_name' not in cls.__dict__:
            cls.collection_name = cls.__name__.lower() + 's'
        if 'coll_path' not in cls.__dict__:
            cls.coll_path = (cls,)


    def __init__(self, session=None, _incoming_data=False, **kwdict):

        self.persisted = _incoming_data
        """Indicates whether this instance has been persisted, or
        whether it represts a new object.
        """

        self.session = session
        """The session for this object."""

        self.dirty = False
        """Whether this object contains any changes or not."""

        self._path = None
        """The path to this object."""

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
                raise TypeError("__init__() got an unexpected keyword" \
                                    " argument '%s'" % key)
            if(_incoming_data):
                # skip validation and marking
                self.__dict__[f_key].has_value = True
                self.__dict__[f_key]._set(value)
            else:
                self.__dict__[f_key].set(value)


    def save(self):
        """Save this object.

        Note that you'll have to call commit() on the appropriate
        session before the actual save will take place.
        """
        if self.persisted:
            self.session.update(self)
        else:
            self.session.add(self)


    def remove(self):
        """Remove this object from the database.

        Note that you'll have to call commit() on the appropriate
        session before the actual remove will take place.
        """
        self.session.remove(self)


    def prepare_persist(self):
        """Prepare this object for persistence.

        Don't call this method directly unless you know what you're
        doing.
        """
        for value in self.__dict__.itervalues():
            if isinstance(value, field.Field):
                value.prepare_persist()


    def mark_persisted(self):
        """Mark this object as having been already persisted.

        Don't call this method directly unless you know what you're
        doing.
        """
        self.persisted = True

        # Why was this here??
        # self._path = None

        for value in self.__dict__.itervalues():
            if isinstance(value, field.Field):
                value.mark_clean()


    def path(self):
        """Returns the path of this object."""
        if self._path is not None:
            return self._path
        else:
            return self.coll_path + (self.primary_key,)


    def __repr__(self):
        return "%s(%s)" % (
            self.__class__.__name__,
            ', '.join(set([
                        "%s=%s" % (value.key, repr(value))
                        for value in filter(lambda x: isinstance(x, field.Field) and not isinstance(x, field.Foreign),
                                            self.__dict__.values())])))
