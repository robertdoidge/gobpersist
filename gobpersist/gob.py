# gob.py - an individual "relation"/"tuple"/"object"/etc.
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
"""Gobs -- objects that will persist in the database.

.. moduleauthor:: Evan Buswell <evan.buswell@accellion.com>
"""

import gobpersist.field

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

    The default is ``(class_key, primary_key)``.
    """

    coll_key = None
    """Key for objects of this collection.

    The default is ``(class_key,)``.  Note that by default no objects
    will be stored here; you must add it to the return value of keyset
    when that is appropriate.
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

    consistency = []
    """Consistency requirements (triggers) for a given object.

    This should be a list of dictionaries, with the following values:

    * ``'field'`` - the local field with consistency requirements.

    * ``'foreign_class'`` - the class of the foreign object(s).

    * ``'foreign_obj'`` - a key to obtain the foreign object(s) which
      must be kept consistent with this object.

    * ``'foreign_field'`` - the field on the foreign object(s) which
      must be kept consistent.

    * ``'update'`` (triggered when the field is updated) - one of
      ``'cascade'`` (update the foreign field(s)), ``'set_null'`` (set
      the foreign field to ``None``), ``'set_default'`` (set the
      foreign field to a default value), or ``None`` (take no action).

    * ``'remove'`` (triggered when the object is removed from the db)
      - one of ``'cascade'`` (delete the foreign object(s)),
      ``'set_null'`` (set the foreign field to ``None``),
      ``'set_default'`` (set the foreign field to a default value), or
      ``None`` (take no action).

    * ``'invalidate'`` (triggered when the object in the cache is
      invalidated) - either ``'cascade'`` (invalidate the foreign
      object(s)), or ``None`` (take no action).
    """

    set_consistency = []
    """Consistency requirements (triggers) for a given object,
    pertaining to sets of keys rather than individual keys.

    This should be a list of dictionaries, with the following values:

    * ``'field'`` - the local field with consistency requirements.

    * ``'foreign_class'`` - the class of the foreign object(s).

    * ``'foreign_obj'`` - a key to obtain the foreign object(s) which
      must be kept consistent with this object.

    * ``'foreign_field'`` - the set field on the foreign object(s)
      which must be kept consistent.

    * ``'foreign_value'`` - the value in the foreign set which
      corresponds to this object.

    * ``'update'`` (triggered when the field is updated) - one of
      ``'cascade'`` (update the foreign set(s)), ``'remove'`` (remove
      the foreign objects that correspond to the removed keys from the
      db), or ``None`` (take no action).

    * ``'remove'`` (triggered when the object is removed from the db)
      - one of ``'cascade'`` (delete the foreign object(s)),
      ``'update'`` (remove this object reference from the foreign
      set(s)), or ``None`` (take no action).

    * ``'invalidate'`` (triggered when the object in the cache is
      invalidated) - either ``'cascade'`` (invalidate the foreign
      object(s)), or ``None`` (take no action).
    """


    def keyset(self):
        """This function is called to determine the keys under which
        to store this object.

        By default, this method simply returns the ``keys`` list.
        """
        return self.keys

    def unique_keyset(self):
        """This function is called to determine the unique keys under
        which to store this object.

        By default, this method simply returns the ``unique_keys``
        list.
        """
        return self.unique_keys

    @classmethod
    def reload_class(cls):
        """Reload the class as if it was recreated from the
        metaclass."""
        # set up fields
        primary_key = None
        for key, value in cls.__dict__.iteritems():
            if key != 'primary_key' and isinstance(value, gobpersist.field.Field):
                value.instance_key = field_key(key)
                value._name = key
                if value.primary_key:
                    if primary_key:
                        raise ValueError("More than one primary key defined" \
                                             " for class '%s'" % cls.__name__)
                    primary_key = value
                if value.name is None:
                    value.name = key
                if isinstance(value, gobpersist.field.Foreign):
                    if value.foreign_class == 'self':
                        value.foreign_class = cls

        cls.primary_key = primary_key

        for consistence in cls.consistency:
            if consistence['foreign_class'] == 'self':
                consistence['foreign_class'] = cls

        if 'class_key' not in cls.__dict__:
            cls.class_key = cls.__name__.lower() + 's'
        if 'obj_key' not in cls.__dict__:
            cls.obj_key = (cls.class_key, cls.primary_key)
        if 'coll_key' not in cls.__dict__:
            cls.coll_key = (cls.class_key,)


    def __init__(self, session=None, _incoming_data=False, **kwdict):
        """
        Args:
           ``session``: The session for this object.

           ``dirty``: Whether this object contains any changes or not.

           ``_path``: The path to this object.

           The remainder of the arguments are interpreted as initial
           values for the fields in this gob.
        """

        self.persisted = _incoming_data
        """Indicates whether this instance has been persisted, or
        whether it represents a new object."""

        self.session = session
        """The session for this object."""

        self.dirty = False
        """Whether this object contains any changes or not."""

        self._path = None
        """The path to this object."""

        # make local copies of fields
        for key in dir(self.__class__):
            value = getattr(self.__class__, key)
            if isinstance(value, gobpersist.field.Field):
                value = value.clone()
                value.instance = self
                self.__dict__[value.instance_key] = value
                if value.primary_key:
                    self.__dict__['primary_key'] = value

        # make foreign fields refer to local fields
        for key in dir(self):
            value = getattr(self, key)
            if isinstance(value, gobpersist.field.Foreign) and value.key is not None:
                value.key = tuple([
                        self.__dict__[keyelem.instance_key] \
                                    if isinstance(keyelem, gobpersist.field.Field) \
                                else keyelem \
                            for keyelem in value.key])

        # make indices refer to local fields
        self.keys \
            = [tuple([self.__dict__[keyelem.instance_key] \
                                  if isinstance(keyelem, gobpersist.field.Field) \
                                  and keyelem.instance is None \
                              else keyelem \
                          for keyelem in path]) \
                   for path in self.keys]
        self.unique_keys \
            = [tuple([self.__dict__[keyelem.instance_key] \
                                  if isinstance(keyelem, gobpersist.field.Field) \
                                  and keyelem.instance is None \
                              else keyelem \
                          for keyelem in path]) \
                   for path in self.unique_keys]
        self.obj_key \
            = tuple([self.__dict__[keyelem.instance_key] \
                                 if isinstance(keyelem, gobpersist.field.Field) \
                                 and keyelem.instance is None \
                             else keyelem \
                         for keyelem in self.obj_key])

        # make consistency rules refer to local fields
        self.consistency \
            = [{'field': self.__dict__[consistence['field'].instance_key] \
                        if 'field' in consistence \
                        and isinstance(consistence['field'], gobpersist.field.Field) \
                        and consistence['field'].instance is None \
                    else consistence['field'] \
                        if 'field' in consistence \
                    else None,
                'foreign_class': consistence['foreign_class'],
                'foreign_obj': \
                    tuple([self.__dict__[keyelem.instance_key] \
                                       if isinstance(keyelem, gobpersist.field.Field) \
                                       and keyelem.instance is None \
                                   else keyelem \
                               for keyelem in consistence['foreign_obj']]),
                'foreign_field': consistence['foreign_field'] \
                        if 'foreign_field' in consistence \
                    else None,
                'update': consistence['update'] \
                        if 'update' in consistence \
                    else None,
                'remove': consistence['remove'] \
                        if 'remove' in consistence \
                    else None,
                'invalidate': consistence['invalidate'] \
                        if 'invalidate' in consistence \
                    else None}
               for consistence in self.consistency]

        self.set_consistency \
            = [{'field': self.__dict__[consistence['field'].instance_key] \
                        if 'field' in consistence \
                        and isinstance(consistence['field'], gobpersist.field.Field) \
                        and consistence['field'].instance is None \
                    else consistence['field'] \
                        if 'field' in consistence \
                    else None,
                'foreign_class': consistence['foreign_class'],
                'foreign_obj': \
                    tuple([self.__dict__[keyelem.instance_key] \
                                       if isinstance(keyelem, gobpersist.field.Field) \
                                       and keyelem.instance is None \
                                   else keyelem \
                               for keyelem in consistence['foreign_obj']]),
                'foreign_field': consistence['foreign_field'] \
                        if 'foreign_field' in consistence \
                    else None,
                'foreign_value': self.__dict__[consistence['foreign_value'].instance_key] \
                        if 'foreign_value' in consistence \
                        and isinstance(consistence['foreign_value'], gobpersist.field.Field) \
                        and consistence['foreign_value'].instance is None \
                    else consistence['foreign_value'] \
                        if 'foreign_value' in consistence \
                    else None,
                'update': consistence['update'] \
                        if 'update' in consistence \
                    else None,
                'remove': consistence['remove'] \
                        if 'remove' in consistence \
                    else None,
                'invalidate': consistence['invalidate'] \
                        if 'invalidate' in consistence \
                    else None}
               for consistence in self.set_consistency]

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

        Note that you'll have to call
        :func:`gobpersist.session.Session.commit` on the appropriate
        session before the actual save will take place.
        """
        if self.persisted:
            self.session.update(self)
        else:
            self.session.add(self)


    def remove(self):
        """Remove this object from the database.

        Note that you'll have to call
        :func:`gobpersist.session.Session.commit` on the appropriate
        session before the actual remove will take place.
        """
        self.session.remove(self)


    def prepare_add(self):
        """Prepares this object to be added to the store.

        Don't call this method directly unless you know what you're
        doing.
        """
        for value in self.__dict__.itervalues():
            if isinstance(value, gobpersist.field.Field):
                value.prepare_add()


    def prepare_update(self):
        """Prepares this object to be updated in the store.

        Don't call this method directly unless you know what you're
        doing.
        """
        for value in self.__dict__.itervalues():
            if isinstance(value, gobpersist.field.Field):
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
        that you must use
        :func:`gobpersist.session.Session.rollback`."""
        for value in self.__dict__.itervalues():
            if isinstance(value, gobpersist.field.Field):
                value.revert()


    def mark_persisted(self):
        """Mark this object as having been already persisted.

        Don't call this method directly unless you know what you're
        doing.
        """
        self.persisted = True
        self.dirty = False

        for value in self.__dict__.itervalues():
            if isinstance(value, gobpersist.field.Field):
                value.mark_persisted()


    @classmethod
    def initialize_db(cls, session):
        """Put the minimum requisite entries in the database such that
        the semantics of the schema function properly.

        Note that schema initialization is still rather dumb.  This
        will just overwrite anything that's in the database, so only
        use it for true initialization."""
        keys = cls.keys[:] # Why did I copy this before I used it to create a
        keys = set(keys)   # set which is also a copy???
        keys.add(cls.coll_key)
        for key in keys:
            simple = True
            for entry in keys:
                if isinstance(entry, gobpersist.field.Field):
                    simple = False
                    break
            if simple:
                session.add_collection(key)

    def __repr__(self):
        # Because Python should be lisp?  I dunno...
        return "%s(%s)" % (
            self.__class__.__name__,
            ', '.join(
                set(["%s=%s" % (value._name, repr(value)) \
                         for value in filter(
                            lambda x: isinstance(x, gobpersist.field.Field) \
                                and not isinstance(x, gobpersist.field.Foreign),
                            self.__dict__.values())] \
                    + ["keys=[%s]" \
                       % ', '.join(["(%s)" % ', '.join([keyelem.coll_name if isinstance(keyelem, Gob) \
                                                            else "%s=%s" % (keyelem._name, repr(keyelem.value)) \
                                                                if isinstance(keyelem, gobpersist.field.Field)
                                                        else repr(keyelem)
                                                        for keyelem in key]) \
                                        for key in self.keys]),
                       "unique_keys=[%s]" \
                       % ', '.join(["(%s)" % ', '.join([keyelem.coll_name if isinstance(keyelem, Gob) \
                                                            else "%s=%s" % (keyelem._name, repr(keyelem.value)) \
                                                                if isinstance(keyelem, gobpersist.field.Field) \
                                                            else repr(keyelem) \
                                                            for keyelem in key]) \
                                        for key in self.unique_keys]),
                       "consistency=[%s]" \
                       % ', '.join([ \
                           "{'field': %s, 'foreign_obj': %s, 'foreign_field': %s," \
                           " 'update': %s, 'remove': %s, 'invalidate': %s}" \
                           % ("%s=%s" % (consistence['field']._name, \
                                             repr(consistence['field'].value)) \
                                      if isinstance(consistence['field'], gobpersist.field.Field) \
                                  else repr(consistence['field']),
                              "(%s)" % ', '.join([keyelem.coll_name if isinstance(keyelem, Gob) \
                                                            else "%s=%s" % (keyelem._name, repr(keyelem.value)) \
                                                                if isinstance(keyelem, gobpersist.field.Field) \
                                                            else repr(keyelem) \
                                                            for keyelem in consistence['foreign_obj']]),
                              repr(consistence['foreign_field']),
                              repr(consistence['update']),
                              repr(consistence['remove']),
                              repr(consistence['invalidate'])) \
                           for consistence in self.consistency]),
                       "set_consistency=[%s]" \
                       % ', '.join([ \
                           "{'field': %s, 'foreign_obj': %s, 'foreign_field': %s," \
                           " 'foreign_value': %s, 'update': %s, 'remove': %s," \
                           " 'invalidate': %s}" \
                           % ("%s=%s" % (consistence['field']._name, \
                                             repr(consistence['field'].value)) \
                                      if isinstance(consistence['field'], gobpersist.field.Field)
                                  else repr(consistence['field']),
                              "(%s)" % ', '.join([keyelem.coll_name if isinstance(keyelem, Gob) \
                                                            else "%s=%s" % (keyelem._name, repr(keyelem.value)) \
                                                                if isinstance(keyelem, gobpersist.field.Field) \
                                                            else repr(keyelem) \
                                                            for keyelem in consistence['foreign_obj']]),
                              repr(consistence['foreign_field']),
                              "%s=%s" % (consistence['foreign_value']._name, \
                                             repr(consistence['foreign_value'].value)) \
                                      if isinstance(consistence['foreign_value'], gobpersist.field.Field) \
                                  else repr(consistence['foreign_value']),
                              repr(consistence['update']),
                              repr(consistence['remove']),
                              repr(consistence['invalidate'])) \
                           for consistence in self.set_consistency])])))
