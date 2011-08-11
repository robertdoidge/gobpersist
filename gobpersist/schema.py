from __future__ import absolute_import
from . import gob
from . import field

class SchemaCollection(object):
    """An object representing a collection in a given schema.

    This is basically just syntactic sugar for making queries.  But
    maybe not completely.

    The 'list' method takes a simpler query language than the session.
    You query using a series of key-value pairs, where the keys are
    the names of the variables for the object and the values are the
    values which must be equal to those objects, for example:

    sc.list(name='abc123')

    You can use other comparators than 'eq' by using tuples, for
    example:

    sc.list(name='abc123', num_users=('gt', 20))

    The full query language is available by the _query parameter.
    """
    def __init__(self, cls, session, key, sticky=None, autoset=None):
        self.cls = cls
        """The class of which objects of this SchemaCollection are
        instances."""

        self.session = session
        """The session for this SchemaCollection."""

        self.key = key
        """The key for this SchemaCollection."""

        self.sticky = sticky
        """A query fragment which is always added to the query."""

        self.autoset = autoset
        """A dictionary of key-value pairs that will be set on each
        added item."""


    def _translate_qelem(self, k, v):
        """Translate an element of a query into normal query
        format."""
        if k == 'and':
            return self._translate_query(v)
        elif k == 'or':
            ret = {'or': []}
            for k2, v2 in v.iteritems():
                ret['or'].append(self._translate_qelem(k2, v2))
        elif isinstance(v, tuple):
            return {v[0]: [(k,), v[1]]}
        else:
            return {'eq': [(k,), v]}


    def _translate_and(self, query):
        """Translate an 'and' query into normal query format."""
        ret = {'and' : []}
        for k, v in query.iteritems():
            ret['and'].append(self._translate_qelem(k, v))
        return ret


    def _translate_query(self, query):
        """Translate a query into the normal query format."""
        ret = self._translate_and(query)
        if self.sticky is not None:
            ret['and'].append(self.sticky)
        return ret


    def list(self, _query=None, **kwargs):
        """List items in the collection.

        See the documentation for this class for an explanation of the
        query language.
        """
        if _query is None:
            _query = self._translate_query(kwargs)
        return self.session.query(cls=self.cls, key=self.key, query=_query)


    def get(self, primary_key):
        """Get an item with a specific primary key."""
        key = []
        for keyelem in self.cls.obj_key:
            if isinstance(keyelem, field.Field) \
                    and keyelem.primary_key:
                f = keyelem.clone(clean_break=True)
                f.set(primary_key)
                key.append(f)
            else:
                key.append(keyelem)
        res = self.session.query(cls=self.cls, key=tuple(key))
        if res:
            return res[0]
        else:
            return None


    def add(self, gob):
        """Add an item to the collection."""
        gob.session = self.session
        if self.autoset is not None:
            for key, value in self.autoset.iteritems():
                if isinstance(value, field.Field):
                    setattr(gob, key, value.value)
                else:
                    setattr(gob, key, value)
        gob.save()


    def update(self, gob):
        """Update item.

        This is equivalent to calling gob.save()
        """
        gob.session = self.session
        gob.save()


    def remove(self, gob):
        """Remove item.

        This is equivalent to calling gob.remove()
        """
        gob.session = self.session
        gob.remove()


    def initialize_db(self):
        """Put the minimum requisite entries in the database such that
        the semantics of the schema function properly.

        Note that schema initialization is still rather dumb.  This
        will just overwrite anything that's in the database, so only
        use it for true initialization."""
        self.session.add_collection(self.key)
        self.cls.initialize_db(self.session)
        self.session.commit()


class Schema(object):
    """Convenience class to hold a schema.

    Use this class by subclassing it and setting variables equal to
    the Gob classes you want in your schema.
    """
    def __init__(self, session):

        self.session = session
        """The session for this Schema"""

        for name in dir(self):
            collection = getattr(self, name)
            if isinstance(collection, type) \
                    and issubclass(collection, gob.Gob):
                setattr(self, name, SchemaCollection(cls=collection,
                                                     session=session,
                                                     key=collection.coll_key))

    def __getattr__(self, name):
        return getattr(self.session, name)

    def collection_for_key(self, key):
        """Create a collection that corresponds to a given key."""
        return SchemaCollection(self.session, key)

    def initialize_db(self):
        """Put the minimum requisite entries in the database such that
        the semantics of the schema function properly.

        Note that schema initialization is still rather dumb.  This
        will just overwrite anything that's in the database, so only
        use it for true initialization."""
        for name in dir(self):
            collection = getattr(self, name)
            if isinstance(collection, SchemaCollection):
                collection.initialize_db()
