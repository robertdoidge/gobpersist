from __future__ import absolute_import
from . import field

import re
import types

class QueryError(Exception):
    """Raised when a malformed query is detected."""

class backend_delegable(object):
    def __init__(self, f):
        self.orig_f = f
    def __get__(self, instance, owner):
        delegated_f = getattr(instance.backend, self.orig_f.__name__, None)
        if delegated_f is not None:
            return delegated_f
        return types.MethodType(self.orig_f, instance, instance.__class__)

class BackendDelegatorMeta(type):
    def __init__(cls, *args, **kwargs):
        add_dict = {}
        for key, value in cls.__dict__.iteritems():
            if isinstance(value, backend_delegable):
                add_dict['_' + key] = value.orig_f
        for key, value in add_dict.iteritems():
            setattr(cls, key, value)
        super(BackendDelegatorMeta, cls).__init__(*args, **kwargs)

class Session(object):
    """Generic session object.  Delegates whatever possible to its backend"""

    __metaclass__ = BackendDelegatorMeta

    def __init__(self, backend, storage_engine=None):
        self.collections = {}
        self.operations = {
            'additions': set(),
            'removals': set(),
            'updates': set()
            }
        self.backend = backend
        self.backend.session = self
        if storage_engine is not None:
            self.storage_engine = storage_engine
            self.storage_engine.session = self

    @backend_delegable
    def register_gob(self, gob):
        if gob.collection_name not in self.collections:
            self.collections[gob.collection_name] = {}
        self.collections[gob.collection_name][gob.primary_key] = gob

    @backend_delegable
    def add(self, gob):
        gob.prepare_persist()
        self._register_gob(gob)
        self.operations['additions'].add(gob)

    @backend_delegable
    def update(self, gob):
        gob.prepare_persist()
        self.operations['updates'].add(gob)

    @backend_delegable
    def remove(self, gob):
        gob.prepare_persist()
        self.operations['removals'].add(gob)

    @backend_delegable
    def query_to_pquery(self, cls, query):
        ret = {}
        for key, value in query.iteritems():
            if re.match('^(?:eq|ne|gt|lt|gte|lte)$', key):
                newvalue = []
                f = None
                for item in value:
                    if isinstance(item, tuple):
                        # identifier; we need to rename it appropriately.
                        f = getattr(cls, item[0])
                        newvalue.append((f.name,))
                for item in value:
                    if not isinstance(item, tuple):
                        if isinstance(item, field.Field):
                            newvalue.append(self.field_to_pfield(item))
                        elif f is not None:
                            newf = f.clone()
                            newf.modifiable = True
                            newf.instance = None
                            newf.set(item)
                            newvalue.append(self.field_to_pfield(newf))
                        else:
                            newvalue.append(self.value_to_pvalue(item))
                ret[key] = newvalue
            elif re.match('^(?:and|or|nor|not)', key):
                newvalue = []
                for item in value:
                    newvalue.append(self.query_to_pquery(cls, item))
                ret[key] = newvalue
            else:
                raise QueryError("Invalid query operator '%s'" % key)
        return ret

    @backend_delegable
    def path_to_ppath(self, path):
        if len(path) < 1:
            return path
        ret = []
        cls = path[0]
        ret.append(cls.collection_name)
        f = cls.primary_key
        for pathelem in path[1:]:
            if f is None:
                if isinstance(pathelem, field.ForeignCollection):
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
                    newf = f.clone()
                    newf.modifiable = True
                    newf.instance = None
                    newf.set(pathelem)
                    ret.append(self.field_to_pfield(newf))
                f = None
        return tuple(ret)

    def _path_to_cls(self, path):
        if len(path) < 1:
            return None
        cls = path[0]
        for pathelem in path[2::2]:
            if isinstance(pathelem, field.ForeignCollection):
                cls = pathelem.foreign_class
            else:
                cls = getattr(cls, pathelem).foreign_class
        return cls

    @backend_delegable
    def retrieve_to_pretrieve(self, cls, retrieve):
        return set([getattr(cls, retrieval).name for retrieval in retrieve])

    @backend_delegable
    def update_query(self, path, set, query=None):
        for item in self.query(path, query, retrieve=[]):
            for k, v in set:
                setattr(item, k, v)
            item.save()

    @backend_delegable
    def remove_query(self, path, query):
        for item in self.query(path, query, retrieve=[]):
            item.remove()

    @backend_delegable
    def query(self, path, query=None, retrieve=None, offset=None, limit=None):
        cls = self._path_to_cls(path)
        if retrieve is not None:
            retrieve.append('primary_key')
            for k, v in cls.__dict__:
                if isinstance(v, field.Field) and v.revision_tag:
                    retrieve.append(k)
            retrieve = self.retrieve_to_pretrieve(cls, retrieve)
        path = self.path_to_ppath(path)
        if query is not None:
            query = self.query_to_pquery(cls, query)
        if cls.collection_name not in self.collections:
            self.collections[cls.collection_name] = {}
        results = [self._create_gob(cls, result) for result in self.do_query(path, query, retrieve, offset, limit)]
        ret = []
        for gob in results:
            if gob.primary_key in self.collections[gob.collection_name]:
                self._update_object(self.collections[gob.collection_name][gob.primary_key], gob)
                ret.append(self.collections[gob.collection_name][gob.primary_key])
            else:
                self.collections[gob.collection_name][gob.primary_key] = gob
                ret.append(gob)
        return ret

    def _create_gob(self, cls, dictionary):
        return cls(self, _incoming_data=True, **dictionary)
    
    def _update_object(self, gob, updater, force=False):
        for value in gob.__dict__.itervalues():
            if isinstance(value, field.Field) and (not value.dirty or force):
                value.value = updater.__dict__[value._key].value

    @backend_delegable
    def do_query(self, path, query, retrieve, offset, limit):
        # Must be defined for a subclass
        # returns a list of dicts representing each object
        raise NotImplementedError("Backend type '%s' does not implement do_query" % type(self.backend))

    @backend_delegable
    def do_commit(self, operations):
        # Must be defined for a subclass
        raise NotImplementedError("Backend type '%s' does not implement do_commit" % type(self.backend))

    @backend_delegable
    def field_to_pfield(self, f):
        return self.value_to_pvalue(f)

    @backend_delegable
    def gob_to_pgob(self, gob):
        pgob = {}
        for f in gob.__dict__.itervalues():
            if isinstance(f, field.Field):
                pgob[f.name] = self.field_to_pfield(f)
        return pgob

    @backend_delegable
    def value_to_pvalue(self, value):
        if isinstance(value, field.Field):
            value = value.value
        if isinstance(value, (list, set)):
            return [self.field_to_pfield(item) if isinstance(item, field.Field) else self.value_to_pvalue(item) for item in value]
        else:
            return value

    @backend_delegable
    def commit(self):
        operations = {}
        operations['additions'] = []
        for gob in self.operations['additions']:
            op = {
                'path': self.path_to_ppath(gob.path()),
                'item': {},
                'gob': gob
                }
            for f in gob.__dict__.itervalues():
                if isinstance(f, field.Field) and f.modifiable:
                    op['item'][f.name] = self.field_to_pfield(f)
            operations['additions'].append(op)

        operations['updates'] = []
        for gob in self.operations['updates']:
            op = {
                'path': self.path_to_ppath(gob.path()),
                'item': {},
                'gob': gob
                }
            for f in gob.__dict__.itervalues():
                if isinstance(f, field.Field):
                    if f.dirty:
                        op['item'][f.name] = self.field_to_pfield(f)
                    if f.revision_tag:
                        if 'conditions' not in op:
                            op['conditions'] = {'and': []}
                        op['conditions']['and'].append({'lte': [(f.name,), self.field_to_pfield(f)]})
            operations['updates'].append(op)

        operations['removals'] = []
        for gob in self.operations['removals']:
            op = {
                'path': self.path_to_ppath(gob.path())
                }
            for f in gob.__dict__.itervalues():
                if isinstance(f, field.Field) and f.revision_tag:
                    if 'conditions' not in op:
                        op['conditions'] = {'and': []}
                    op['conditions']['and'].append({'lte': [(f.name,), self.field_to_pfield(f)]})
            operations['removals'].append(op)

        for gob, gobdict in self.do_commit(operations).iteritems():
            gob.mark_persisted()
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

    @backend_delegable
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

    @backend_delegable
    def upload(self, gob, fp):
        gobdict = self.storage_engine.do_upload(self.gob_to_pgob(gob), fp)
        self._update_object(gob, self._create_gob(gob.__class__, gobdict), True)

    @backend_delegable
    def upload_iter(self, gob, fp):
        gobdict = self.storage_engine.do_upload_iter(self.gob_to_pgob(gob), fp)
        self._update_object(gob, self._create_gob(gob.__class__, gobdict), True)

    @backend_delegable
    def download(self, gob):
        gobdict, iterable = self.storage_engine.do_download(self.gob_to_pgob(gob))
        self._update_object(gob, self._create_gob(gob.__class__, gobdict), True)
        return iterable
