from __future__ import absolute_import
from . import field

import types

class QueryError(Exception):
    """Raised when a malformed query is detected."""

class Delegable(object):
    """This is a decorator for functions to be delegated to some other
    member."""
    def __init__(self, f, name):
        self.orig_f = f
        self.name = name
    def __get__(self, instance, owner):
        delegated_f = getattr(getattr(instance, self.name), self.orig_f.__name__, None)
        if delegated_f is not None:
            return delegated_f
        # Note that instance.__class__ may be different than owner...
        return types.MethodType(self.orig_f, instance, instance.__class__)

def delegable(name):
    return lambda f: Delegable(f, name)

class VisibleDelegatorMeta(type):
    """A MetaClass for setting _func_name as the original function for delegated
    functions."""
    def __init__(cls, *args, **kwargs):
        add_dict = {}
        for key, value in cls.__dict__.iteritems():
            if isinstance(value, Delegable):
                add_dict['_' + key] = value.orig_f
        for key, value in add_dict.iteritems():
            setattr(cls, key, value)
        super(VisibleDelegatorMeta, cls).__init__(*args, **kwargs)


class Session(object):
    """Generic session object.  Delegates whatever possible to its backend"""

    __metaclass__ = VisibleDelegatorMeta

    def __init__(self, backend, storage_engine=None):

        self.collections = {}
        """Registry for all items this session currently knows about.

        Populated as [collection_name][obj.primary_key] = obj
        """

        self.operations = {
            'additions': set(),
            'removals': set(),
            'updates': set()
            }
        """The operations currently queued but not yet performed."""

        self.backend = backend
        """The back end for this session."""

        self.storage_engine = storage_engine
        """The storage engine for this session."""

        self.backend.caller = self
        if storage_engine is not None:
            self.storage_engine.caller = self

    @delegable('backend')
    def register_gob(self, gob):
        """Called to add a gob to this session's registry.

        The registry allows for deduplication of search results and such."""
        if gob.collection_name not in self.collections:
            self.collections[gob.collection_name] = {}
        self.collections[gob.collection_name][gob.primary_key] = gob

    @delegable('backend')
    def add(self, gob):
        """Persist a new item."""
        gob.prepare_persist()
        self._register_gob(gob)
        self.operations['additions'].add(gob)

    @delegable('backend')
    def update(self, gob):
        """Update an existing item."""
        gob.prepare_persist()
        self.operations['updates'].add(gob)

    @delegable('backend')
    def remove(self, gob):
        """Remove an item."""
        gob.prepare_persist()
        self.operations['removals'].add(gob)

    @delegable('backend')
    def query_to_pquery(self, cls, query):
        """Transform a query into a query that is more palatable to the
        backend."""
        ret = {}
        for key, value in query.iteritems():
            if key in ('eq', 'ne', 'gt', 'lt', 'gte', 'lte'):
                newvalue = []
                f = None
                pass2 = []
                for item in value:
                    if isinstance(item, tuple):
                        # identifier
                        identifier, f = self.idnt_to_pidnt(item, cls)
                        newvalue.append(identifier)
                    elif isinstance(item, dict):
                        # quantifier
                        if len(item) > 1:
                            raise ValueError("Too many keys in quantifier")
                        newquant = {}
                        k, v = item.items()[0]
                        if k not in ('all', 'any', 'none'):
                            raise ValueError("Invalid key '%s' in quantifier" \
                                                 % k)
                        identifier = self.idnt_to_pidnt(item, cls)
                        f = self._idnt_to_field(item, cls)
                        newquant[k] = identifier
                        newvalue.append(identifier)
                    else:
                        # literal
                        if isinstance(item, field.Field):
                            newvalue.append(self.field_to_pfield(item))
                        elif f is not None:
                            newf = f.clone(clean_break=True)
                            newf.set(item)
                            newvalue.append(self.field_to_pfield(newf))
                        else:
                            pass2.append(item)
                for item in pass2:
                    # literal
                    if f is not None:
                        newf = f.clone(clean_break=True)
                        newf.set(item)
                        newvalue.append(self.field_to_pfield(newf))
                    else:
                        newvalue.append(self.value_to_pvalue(item))

                ret[key] = newvalue
            elif key in ('and', 'or', 'nor', 'not'):
                newvalue = []
                for item in value:
                    newvalue.append(self.query_to_pquery(cls, item))
                ret[key] = newvalue
            else:
                raise QueryError("Invalid query operator '%s'" % key)
        return ret

    def _idnt_to_field(self, idnt, cls):
        """Returns the Field that corresponds to an identifier."""
        retfield = None
        for pathelem in idnt:
            if cls is None:
                raise ValueError("Could not find class for element %s of" \
                                     " identifier %s" \
                                     % (repr(pathelem), repr(idnt)))
            if not isinstance(pathelem, field.Field):
                pathelem = getattr(cls, pathelem)
            retfield = pathelem
            if isinstance(pathelem, field.Foreign):
                cls = pathelem.foreign_class
            else:
                # This should be the last element
                cls == None
        return retfield

    @delegable('backend')
    def idnt_to_pidnt(self, idnt, cls):
        """Turns a identifier into something more palatable to the backend."""
        ret = []
        for pathelem in idnt:
            if cls is None:
                raise ValueError("Could not find class for element %s of" \
                                     " identifier %s" \
                                     % (repr(pathelem), repr(idnt)))
            if not isinstance(pathelem, field.Field):
                pathelem = getattr(cls, pathelem)
            retfield = pathelem
            ret += pathelem.name
            if isinstance(pathelem, field.Foreign):
                cls = pathelem.foreign_class
            else:
                # This should be the last element
                cls = None
        return tuple(ret)

    @delegable('backend')
    def path_to_ppath(self, path):
        """Transforms a path into something more palatable to the
        backend."""
        if len(path) == 0:
            # I don't know why this would make sense, but since we can
            # technically parse it, let the backend deal with it.
            return path
        ret = []
        cls = path[0]
        ret.append(cls.collection_name)
        f = cls.primary_key
        for pathelem in path[1:]:
            if f is None:
                if isinstance(pathelem, field.Foreign):
                    f = pathelem
                else:
                    f = getattr(cls, pathelem)
                cls = f.foreign_class
                ret.append(f.name)
                f = getattr(cls, f.foreign_key)
            else:
                if isinstance(pathelem, field.Field):
                    ret.append(self.field_to_pfield(pathelem))
                else:
                    newf = f.clone(clean_break=True)
                    newf.set(pathelem)
                    ret.append(self.field_to_pfield(newf))
                f = None
        return tuple(ret)

    def _path_to_cls(self, path):
        """Returns the class for a given path."""
        if len(path) < 1:
            return None
        cls = path[0]
        for pathelem in path[2::2]:
            if isinstance(pathelem, field.Foreign):
                cls = pathelem.foreign_class
            else:
                cls = getattr(cls, pathelem).foreign_class
        return cls

    @delegable('backend')
    def retrieve_to_pretrieve(self, cls, retrieve):
        """Transform retrieve request into something more palatable for the
        backend."""
        ret = set()
        for retrieval in retrieve:
            if not isinstance(retrieval, field.Field):
                retrieval = getattr(cls, retrieval)
            ret.add(retrieval.name)
        return ret

    @delegable('backend')
    def order_to_porder(self, cls, order):
        """Transform order into something more palatable for the backend."""
        ret = []
        for ordering in order:
            if not isinstance(ordering, dict) or not len(ordering) == 1:
                raise ValueError("Invalid ordering: %s" % repr(ordering))
            key, ordering = ordering.items()[0]
            if key not in ('asc', 'desc'):
                raise ValueError("Invalid key '%s' in ordering %s" \
                                     % (key, repr(ordering)))
            if not isinstance(ordering, field.Field):
                ordering = getattr(cls, ordering)
            ret.add({ key : ordering.name })
        return ret

    @delegable('backend')
    def query(self, path, query=None, retrieve=None, order=None, offset=None, limit=None):
        """Perform a query against the database.

        Backends should be careful in overriding this method, as there
        is some deduplication magic.
        """
        cls = self._path_to_cls(path)
        if retrieve is not None:
            # Should we be doing this?  Maybe the caller should get blank
            # revision_tags if that's what they want.
            retrieve.append(cls.primary_key)
            for k in dir(cls):
                v = getattr(cls, k)
                if isinstance(v, field.Field) and v.revision_tag:
                    retrieve.append(k)
            retrieve = self.retrieve_to_pretrieve(cls, retrieve)
        path = self.path_to_ppath(path)
        if query is not None:
            query = self.query_to_pquery(cls, query)
        if cls.collection_name not in self.collections:
            self.collections[cls.collection_name] = {}
        results = [self._create_gob(cls, result) \
                       for result in self.do_query(path, query, retrieve, offset, limit)]
        ret = []
        for gob in results:
            if gob.primary_key in self.collections[gob.collection_name]:
                self._update_object(self.collections[gob.collection_name][gob.primary_key],
                                    gob)
                ret.append(self.collections[gob.collection_name][gob.primary_key])
            else:
                self.collections[gob.collection_name][gob.primary_key] = gob
                ret.append(gob)
        return ret

    def _create_gob(self, cls, dictionary):
        """Create a gob from a dictionary.

        TODO: This should retransform the keys it gets into their appropriate
        values based on the field.name attributes in the class.
        """
        return cls(self, _incoming_data=True, **dictionary)
    
    def _update_object(self, gob, updater, force=False):
        """Updates an object to have the values of another one.

        Dirty values are not overwritten, unless force is True.
        """
        for key in dir(gob):
            value = getattr(gob, key)
            if isinstance(value, field.Field) \
                    and not isinstance(value, field.Foreign) \
                    and (not value.dirty or force):
                value.value = updater.__dict__[value._key].value

    @delegable('backend')
    def do_query(self, path, query, retrieve, offset, limit):
        """Actually perform the query.

        All backends must provide this method.  Should return a list
        of dicts representing each object.
        """
        raise NotImplementedError("Backend type '%s' does not implement" \
                                      " do_query" % type(self.backend))

    @delegable('backend')
    def do_commit(self, operations):
        """Actually perform the commit.

        All backends must provide this method.  Should return a list
        of dicts with each updated object dict keyed by the value of
        the 'gob' key provided in input.  (Or an empty dict for no
        updates).
        """
        raise NotImplementedError("Backend type '%s' does not implement" \
                                      " do_commit" % type(self.backend))

    @delegable('backend')
    def field_to_pfield(self, f):
        """Transform a field into a value appropriate for the backend.

        Serialization should be done in here or in value_to_pvalue.
        """
        return self.value_to_pvalue(f)

    @delegable('backend')
    def gob_to_pgob(self, gob):
        """Turn a gob into a dictionary appropriate for the
        backend.
        """
        pgob = {}
        for key in dir(gob):
            f = getattr(gob, key)
            if isinstance(f, field.Field):
                pgob[f.name] = self.field_to_pfield(f)
        return pgob

    @delegable('backend')
    def value_to_pvalue(self, value):
        """Transform a value into a value appropriate for the backend.

        Serialization should be done in here or in value_to_pvalue.
        """
        if isinstance(value, field.Field):
            value = value.value
        if isinstance(value, (list, set)):
            return [self.field_to_pfield(item) if isinstance(item, field.Field) \
                        else self.value_to_pvalue(item) \
                        for item in value]
        else:
            return value

    @delegable('backend')
    def commit(self):
        """Commit all pending changes.

        Backends overriding this method should be careful of
        deduplication efforts.
        """
        operations = {}
        operations['additions'] = []
        for gob in self.operations['additions']:
            op = {
                'path': self.path_to_ppath(gob.path()),
                'item': {},
                'gob': gob
                }
            for key in dir(gob):
                f = getattr(gob, key)
                if isinstance(f, field.Field):
                    op['item'][f.name] = self.field_to_pfield(f)
            operations['additions'].append(op)

        operations['updates'] = []
        for gob in self.operations['updates']:
            op = {
                'path': self.path_to_ppath(gob.path()),
                'item': {},
                'gob': gob
                }
            for key in dir(gob):
                f = getattr(gob, key)
                if isinstance(f, field.Field):
                    if f.dirty:
                        op['item'][f.name] = self.field_to_pfield(f)
                    if f.revision_tag:
                        if 'conditions' not in op:
                            op['conditions'] = {'and': []}
                        op['conditions']['and'].append(
                            {'eq': [(f.name,), self.field_to_pfield(f)]})
            operations['updates'].append(op)

        operations['removals'] = []
        for gob in self.operations['removals']:
            op = {
                'path': self.path_to_ppath(gob.path())
                }
            for key in dir(gob):
                f = getattr(gob, key)
                if isinstance(f, field.Field) and f.revision_tag:
                    if 'conditions' not in op:
                        op['conditions'] = {'and': []}
                    op['conditions']['and'].append(
                        {'eq': [(f.name,), self.field_to_pfield(f)]})
            operations['removals'].append(op)

        for gob, gobdict in self.do_commit(operations).iteritems():
            # gob.mark_persisted()
            self._update_object(gob, self._create_gob(gob.__class__, gobdict))
            if gob.primary_key not in self.collections[gob.collection_name]:
                self.collections[gob.collection_name][gob.primary_key] = gob

        for operation in self.operations.itervalues():
            for gob in operation:
                gob.mark_persisted()
        self.operations = {
            'additions': set(),
            'removals': set(),
            'updates': set()
            }

    @delegable('backend')
    def rollback():
        """Roll back the transaction.

        Note that although the reference to each object is cleared
        from the internal cache, external references are not cleaned
        up or reverted.
        """
        self.collections = {}
        self.operations = {
            'additions': {},
            'removals': {},
            'updates': {}
            }

    @delegable('storage_engine')
    def do_upload(pgob, fp):
        """Storage engines should provide this method.

        TODO: Provide a default version that wraps the file in an
        iterable object.

        Should return a dictionary representing the new / changed
        object.
        """
        raise NotImplementedError("Storage engine type '%s' does not" \
                                      " implement do_upload" \
                                      % type(self.storage_engine))

    @delegable('storage_engine')
    def do_download(pgob, fp):
        """Storage engines should provide this method.

        Should return a tuple of the dictionary representing the
        object, and an iterable containing the data.
        """
        raise NotImplementedError("Storage engine type '%s' does not" \
                                      " implement do_download" \
                                      % type(self.storage_engine))

    @delegable('storage_engine')
    def do_upload_iter(pgob, iterable):
        """Storage engines should provide this method.

        Should return a dictionary representing the new / changed
        object.  The default version merely wraps the iterator as a
        file-like object and sends it to do_upload.
        """
        readstr = ""
        class filewrapper(object):
            def read(size):
                while len(readstr) < size:
                    try:
                        readstr += iterable.next()
                    except StopIteration:
                        break
                ret = readstr[:size]
                readstr = readstr[size:]
                return ret
        return filewrapper()
        

    @delegable('storage_engine')
    def upload(self, gob, fp):
        """Upload the data to the gob.

        Returns nothing, but sets the fields on the object if they are
        updated.
        """
        gobdict = self.do_upload(self.gob_to_pgob(gob), fp)
        self._update_object(gob, self._create_gob(gob.__class__, gobdict), True)

    @delegable('storage_engine')
    def upload_iter(self, gob, iterable):
        """Upload the data to the gob, iterable version.

        Returns nothing, but sets the fields on the object if they are
        updated.
        """
        gobdict = self.do_upload_iter(self.gob_to_pgob(gob), iterable)
        self._update_object(gob, self._create_gob(gob.__class__, gobdict), True)

    @delegable('storage_engine')
    def download(self, gob):
        """Download the file from the gob.

        Returns and iterable containing the data.
        """
        gobdict, iterable = self.do_download(self.gob_to_pgob(gob))
        self._update_object(gob, self._create_gob(gob.__class__, gobdict), True)
        return iterable


class Backend(object):
    """Abstract base class for session back ends."""

    def __init__(self, backend=None):

        self.backend = backend
        """The back end for this back end."""

        self.caller = None
        """The caller of this back end.

        It is the caller's responsibility to set this.
        """

        if backend is not None:
            backend.caller = self

    def __getattr__(self, name):
        upstream = False
        if name[0] == '_':
            upstream = True
            fname = name[1:]
        else:
            fname = name
        sfunc = getattr(Session, fname, None)
        if sfunc is None \
                or not isinstance(sfunc, Delegable) \
                or not sfunc.name == 'backend':
            raise AttributeError
        elif upstream:
            return getattr(self.caller, name)
        else:
            return getattr(self.backend, name)


class StorageEngine(object):
    """Abstract base class for storage engines."""

    def __init__(self, storage_engine=None):

        self.storage_engine = storage_engine
        """The storage engine for this storage engine."""

        self.caller = None
        """The caller of this storage engine.

        It is the caller's responsibility to set this.
        """

        if storage_engine is not None:
            storage_engine.caller = self

    def __getattr__(self, name):
        upstream = False
        if name[0] == '_':
            upstream = True
            fname = name[1:]
        else:
            fname = name
        sfunc = getattr(Session, fname, None)
        if sfunc is None \
                or not isinstance(sfunc, Delegable) \
                or not sfunc.name == 'storage_engine':
            raise AttributeError
        elif upstream:
            return getattr(self.caller, name)
        else:
            return getattr(self.storage_engine, name)
