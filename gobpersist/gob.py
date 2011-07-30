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

    class_key = None
    """Name of the key for this class.

    The default is the name of the class, lower-cased, plus an 's'.
    """

    obj_key = None
    """Primary key for objects of this class.

    The default is (class_key, primary_key).
    """

    coll_key = None
    """Key for objects of this collection.

    The default is (class_key,).  Note that by default no objects will
    be stored here; you must add it to the return value of keyset when
    that is appropriate.
    """

    keys = []
    """The keys under which this object should be stored.

    This should be a list of paths, with the fields to be filled in
    represented by the fields themselves.  The redundancy with
    field.Foreign is well worth the clarity and simplicity of
    implementation.
    """

    unique_keys = []
    """Keys which will uniquely identify this object, in addition to
    the default path + primary_key.

    This should be a list of paths, with the fields to be filled in
    represented by the fields themselves.  The redundancy with
    field.Foreign is well worth the clarity and simplicity of
    implementation.
    """

    def keyset(self):
        """This function is called to determine which keys under which
        to store this object.

        By default, this method simply returns the 'keys' list.
        """
        return self.keys

    def unique_keyset(self):
        """This function is called to determine which keys under which
        to store this object.

        By default, this method simply returns the 'unique_keys' list.
        """
        return self.unique_keys

    @classmethod
    def reload_class(cls):
        """Reload the class as if it was recreated from the metaclass.
        """
        # set up fields
        primary_key = None
        for key, value in cls.__dict__.iteritems():
            if key != 'primary_key' and isinstance(value, field.Field):
                value.instance_key = field_key(key)
                value._name = key
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

        if 'class_key' not in cls.__dict__:
            cls.class_key = cls.__name__.lower() + 's'
        if 'obj_key' not in cls.__dict__:
            cls.obj_key = (cls.class_key, cls.primary_key)
        if 'coll_key' not in cls.__dict__:
            cls.coll_key = (cls.class_key,)


    def __init__(self, session=None, _incoming_data=False, **kwdict):

        self.persisted = _incoming_data
        """Indicates whether this instance has been persisted, or
        whether it represents a new object.
        """

        self.session = session
        """The session for this object."""

        self.dirty = False
        """Whether this object contains any changes or not."""

        self._path = None
        """The path to this object."""

        # make local copies of fields
        for key in dir(self.__class__):
            value = getattr(self.__class__, key)
            if isinstance(value, field.Field):
                value = value.clone()
                value.instance = self
                self.__dict__[value.instance_key] = value
                if value.primary_key:
                    self.__dict__['primary_key'] = value

        # make foreign fields refer to local fields
        for key in dir(self.__class__):
            value = getattr(self.__class__, key)
            if isinstance(value, field.Foreign):
                value.key = tuple([
                        self.__dict__[keyelem.instance_key] \
                                    if isinstance(keyelem, field.Field) \
                                else keyelem \
                            for keyelem in value.key])

        # make indices refer to local fields
        self.keys \
            = [tuple([self.__dict__[keyelem.instance_key] \
                                  if isinstance(keyelem, field.Field) \
                                  and keyelem.instance is None \
                              else keyelem \
                          for keyelem in path]) \
                   for path in self.keys]
        self.unique_keys \
            = [tuple([self.__dict__[keyelem.instance_key] \
                                  if isinstance(keyelem, field.Field) \
                                  and keyelem.instance is None \
                              else keyelem \
                          for keyelem in path]) \
                   for path in self.unique_keys]
        self.obj_key \
            = tuple([self.__dict__[keyelem.instance_key] \
                                 if isinstance(keyelem, field.Field) \
                                 and keyelem.instance is None \
                             else keyelem \
                         for keyelem in self.obj_key])

        # autoset fields according to constructor arguments
        for key, value in kwdict.iteritems():
            f_key = field_key(key)
            if f_key not in self.__dict__:
                raise TypeError("__init__() got an unexpected keyword" \
                                    " argument '%s'" % key)
            if _incoming_data:
                # skip validation...
                self.__dict__[f_key].trip_set()
                self.__dict__[f_key]._set(value)
            else:
                self.__dict__[f_key].set(value)

        if _incoming_data:
            self.mark_persisted()


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


    def prepare_add(self):
        """Prepares this object to be added to the store.

        Don't call this method directly unless you know what you're
        doing.
        """
        for value in self.__dict__.itervalues():
            if isinstance(value, field.Field):
                value.prepare_add()
        

    def prepare_update(self):
        """Prepares this object to be updated in the store.

        Don't call this method directly unless you know what you're
        doing.
        """
        for value in self.__dict__.itervalues():
            if isinstance(value, field.Field):
                value.prepare_update()


    def prepare_delete(self):
        """Prepares this object to be deleted from the store.

        Don't call this method directly unless you know what you're
        doing.
        """
        pass


    def revert(self):
        """Reverts the object to the persisted version.

        This does not clear any pending operations on this object; for
        that you must use session.rollback()."""
        for value in self.__dict__.itervalues():
            if isinstance(value, field.Field):
                value.revert()


    def mark_persisted(self):
        """Mark this object as having been already persisted.

        Don't call this method directly unless you know what you're
        doing.
        """
        self.persisted = True
        self.dirty = False

        for value in self.__dict__.itervalues():
            if isinstance(value, field.Field):
                value.mark_persisted()


    def __repr__(self):
        # Because Python should be lisp?  I dunno...
        return "%s(%s)" % (
            self.__class__.__name__,
            ', '.join(
                set(["%s=%s" % (value._name, repr(value)) \
                         for value in filter(
                            lambda x: isinstance(x, field.Field) \
                                and not isinstance(x, field.Foreign),
                            self.__dict__.values())] \
                    + ["keys=[%s]" \
                       % ', '.join(["(%s)" % ', '.join([keyelem.coll_name if isinstance(keyelem, Gob) \
                                                            else "%s=%s" % (keyelem._name, repr(keyelem.value)) \
                                                                if isinstance(keyelem, field.Field)
                                                        else repr(keyelem)
                                                        for keyelem in key]) \
                                        for key in self.keys]),
                       "unique_keys=[%s]" \
                       % ', '.join(["(%s)" % ', '.join([keyelem.coll_name if isinstance(keyelem, Gob) \
                                                            else "%s=%s" % (keyelem._name, repr(keyelem.value)) \
                                                                if isinstance(keyelem, field.Field) \
                                                            else repr(keyelem) \
                                                            for keyelem in key]) \
                                        for key in self.unique_keys])])))
