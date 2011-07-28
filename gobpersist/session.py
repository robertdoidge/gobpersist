from __future__ import absolute_import
from . import field
from . import gob

class GobTranslator(object):
    """Abstract class to translate gobs for the backend."""

    def gob_to_mygob(self, gob, only_dirty=False):
        """Turn a gob into a object appropriate for the
        backend."""
        mygob = {}
        for key in dir(gob):
            f = getattr(gob, key)
            if isinstance(f, field.Field) \
                    and not isinstance(f, field.Foreign) \
                    and (not only_dirty or f.dirty):
                mygob[f.name] = self.field_to_myfield(f)
        return mygob


    def query_to_myquery(self, cls, query):
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
                        identifier, f = self.idnt_to_myidnt(item, cls)
                        newvalue.append(identifier)
                    elif isinstance(item, dict):
                        # quantifier
                        if len(item) > 1:
                            raise exception.QueryError("Too many keys" \
                                                           " in quantifier")
                        newquant = {}
                        k, v = item.items()[0]
                        if k not in ('all', 'any', 'none'):
                            raise exception.QueryError("Invalid key '%s'" \
                                                           " in quantifier" \
                                                           % k)
                        identifier = self.idnt_to_myidnt(item, cls)
                        f = self.field_for_idnt(cls, item)
                        newquant[k] = identifier
                        newvalue.append(identifier)
                    else:
                        # literal
                        if isinstance(item, field.Field):
                            newvalue.append(self.field_to_myfield(item))
                        elif f is not None:
                            newf = f.clone(clean_break=True)
                            newf.set(item)
                            newvalue.append(self.field_to_myfield(newf))
                        else:
                            pass2.append(item)
                for item in pass2:
                    # literal
                    if f is not None:
                        newf = f.clone(clean_break=True)
                        newf.set(item)
                        newvalue.append(self.field_to_myfield(newf))
                    else:
                        newvalue.append(self.value_to_myvalue(item))

                ret[key] = newvalue
            elif key in ('and', 'or', 'nor', 'not'):
                newvalue = []
                for item in value:
                    newvalue.append(self.query_to_myquery(cls, item))
                ret[key] = newvalue
            else:
                raise exception.QueryError("Invalid query operator '%s'" % key)
        return ret


    def path_to_mypath(self, path, use_persisted_version=False):
        """Transforms a path into something more palatable to the
        backend."""
        if len(path) == 0:
            # I don't know why this would make sense, but since we can
            # technically parse it, let the backend deal with it.
            return path
        ret = []
        cls = path[0]
        if isinstance(cls, type):
            ret.append(cls.collection_name)
            f = cls.primary_key
        else:
            ret.append(cls)
            cls = None
            f = None
        coll = False # whether this loop is for collections or identifiers
        for pathelem in path[1:]:
            if coll:
                if cls is not None and isinstance(pathelem, (str, unicode)):
                    f = getattr(cls, pathelem)
                elif isinstance(pathelem, field.Foreign):
                    f = pathelem
                    cls = f.foreign_class
                    ret.append(f.name)
                    f = getattr(cls, f.foreign_key)
                elif isinstance(pathelem, field.Field):
                    ret.append(self.field_to_myfield(pathelem,
                                                     use_persisted_version))
                else:
                    ret.append(self.value_to_myvalue(pathelem,
                                                     use_persisted_version))
            else:
                if isinstance(pathelem, field.Field):
                    ret.append(self.field_to_myfield(pathelem,
                                                     use_persisted_version))
                elif f is not None:
                    newf = f.clone(clean_break=True)
                    newf.set(pathelem)
                    ret.append(self.field_to_myfield(newf, False))
                else:
                    ret.append(self.value_to_myvalue(pathelem))
                f = None
        return tuple(ret)


    def mypath_to_path(self, cls, mypath):
        """Returns a path given an already transformed path.
        
        TODO: retransform values and identifiers to their previous values.
        """
        return (cls,) + mypath[1:]


    def cls_for_path(self, path):
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


    def retrieve_to_myretrieve(self, cls, retrieve):
        """Transform retrieve request into something more palatable for the
        backend."""
        ret = set()
        for retrieval in retrieve:
            if not isinstance(retrieval, field.Field):
                retrieval = getattr(cls, retrieval)
            ret.add(retrieval.name)
        return ret


    def order_to_myorder(self, cls, order):
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


    def idnt_to_myidnt(self, cls, idnt):
        """Turns an identifier into something more palatable to the
        backend."""
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


    def field_for_idnt(self, cls, idnt):
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


    def field_to_myfield(self, f, use_persisted_version=False):
        """Transform a field into a value appropriate for the backend.

        Serialization should be done in here or in value_to_myvalue.
        """
        return self.value_to_myvalue(f, use_persisted_version)


    def value_to_myvalue(self, value, use_persisted_version=False):
        """Transform a value into a value appropriate for the backend.

        Serialization should be done in here or in value_to_myvalue.
        """
        if isinstance(value, field.Field):
            value = value.value if not use_persisted_version \
                else value.persisted_value
        if isinstance(value, (list, set)):
            return [self.field_to_myfield(item, use_persisted_version) \
                                if isinstance(item, field.Field) \
                            else self.value_to_myvalue(item,
                                                       use_persisted_version) \
                        for item in value]
        else:
            return value


    def mygob_to_gob(self, cls, dictionary):
        """Create a gob from a dictionary.

        TODO: This should retransform the keys it gets into their appropriate
        values based on the field.name attributes in the class.
        """
        return cls(self, _incoming_data=True, **dictionary)


class Session(GobTranslator):
    """Generic session object.  Delegates whatever possible to its backend"""

    def __init__(self, backend, storage_engine=None):

        self.collections = {}
        """Registry for all items this session currently knows about.

        Populated as [collection_name][obj.primary_key] = obj
        """

        self.operations = {
            'additions': set(),
            'removals': set(),
            'updates': set(),
            'collection_additions': set(),
            'collection_removals': set()
            }
        """The operations currently queued but not yet performed."""

        self.paused_transactions = []
        """A list of nested transactions, not including the current
        one.
        """

        self.backend = backend
        """The back end for this session."""

        self.storage_engine = storage_engine
        """The storage engine for this session."""


    def register_gob(self, gob):
        """Called to add a gob to this session's registry.

        The registry allows for deduplication of search results.
        """
        if gob.collection_name not in self.collections:
            self.collections[gob.collection_name] = {}
        self.collections[gob.collection_name][gob.primary_key] = gob


    def add(self, gob):
        """Persist a new item."""
        gob.prepare_add()
        self.register_gob(gob)
        self.operations['additions'].add(gob)


    def update(self, gob):
        """Update an existing item."""
        gob.prepare_update()
        self.operations['updates'].add(gob)


    def remove(self, gob):
        """Remove an item."""
        gob.prepare_delete()
        self.operations['removals'].add(gob)


    def add_collection(self, path):
        """Add an empty collection at path.

        For many backends, this is a no op.
        """
        self.operations['collection_additions'].add(path)


    def remove_collection(self, path):
        """Remove entirely the collection at path."""
        self.operations['collection_removals'].add(path)


    def query(self, path=None, path_range=None, query=None, retrieve=None,
              order=None, offset=None, limit=None):
        """Perform a query against the back end."""
        cls = self.cls_for_path(path)
        if retrieve is not None:
            # Should we be doing this?  Maybe the caller should get blank
            # revision tags if that's what they want.
            retrieve.append(cls.primary_key)
            for k in dir(cls):
                v = getattr(cls, k)
                if isinstance(v, field.Field) and v.revision_tag:
                    retrieve.append(k)
        if cls.collection_name not in self.collections:
            self.collections[cls.collection_name] = {}
        ret = []
        for gob in self.backend.query(path, path_range, query, retrieve,
                                      order, offset, limit):
            if gob.primary_key in self.collections[gob.collection_name]:
                self._update_object(
                    self.collections[gob.collection_name][gob.primary_key],
                    gob)
                ret.append(
                    self.collections[gob.collection_name][gob.primary_key])
            else:
                self.collections[gob.collection_name][gob.primary_key] = gob
                ret.append(gob)
        return ret


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


    def start_transaction(self):
        """Starts a new transaction.

        Since there is always an implicit transaction with gobpersist,
        this actually always starts a *nested* transaction.
        """
        self.paused_transactions.append(self.operations)
        self.operations = {
            'additions': set(),
            'removals': set(),
            'updates': set(),
            'collection_additions': set(),
            'collection_removals': set()
            }


    def commit(self):
        """Commit all pending changes."""
        additions = [{'gob': gob} for gob in self.operations['additions']]

        updates = []
        for gob in self.operations['updates']:
            op = {
                'gob': gob
                }
            for key in dir(gob):
                f = getattr(gob, key)
                if isinstance(f, field.Field) \
                        and f.revision_tag \
                        and f.has_persisted_value:
                    f = f.clone(clean_break = True)
                    f._set(f.persisted_value)
                    if 'conditions' not in op:
                        op['conditions'] = {'and': []}
                    op['conditions']['and'].append(
                        {'eq': [(f.name,), f]})
            updates.append(op)

        removals = []
        for gob in self.operations['removals']:
            op = {
                'gob': gob
                }
            for key in dir(gob):
                f = getattr(gob, key)
                if isinstance(f, field.Field) \
                        and f.revision_tag \
                        and f.has_persisted_value:
                    f = f.clone(clean_break=True)
                    f._set(f.persisted_value)
                    if 'conditions' not in op:
                        op['conditions'] = {'and': []}
                    op['conditions']['and'].append(
                        {'eq': [(f.name,), f]})

        collection_additions = self.operations['collection_additions']
        collection_removals = self.operations['collection_removals']

        for gob in self.backend.commit(additions=additions,
                                       removals=removals,
                                       updates=updates,
                                       collection_additions=collection_additions,
                                       collection_removals=collection_removals):
            # gob.mark_persisted()
            if gob.primary_key in self.collections[gob.collection_name]:
                self._update_object(
                    self.collections[gob.collection_name][gob.primary_key],
                    gob)

        for operation in self.operations.itervalues():
            for gob in operation:
                gob.mark_persisted()
        if len(self.paused_transactions) > 0:
            self.operations = self.paused_transactions.pop()
        else:
            self.operations = {
                'additions': set(),
                'removals': set(),
                'updates': set(),
                'collection_additions': set(),
                'collection_removals': set()
                }


    def rollback(self, revert=False):
        """Roll back the transaction.

        If revert is False (the default), then individual gobs are not
        reverted, and only the list of pending operations is cleared.
        """
        if len(self.paused_transactions) > 0:
            self.operations = self.paused_transactions.pop()
        else:
            self.operations = {
                'additions': set(),
                'removals': set(),
                'updates': set(),
                'collection_additions': set(),
                'collection_removals': set()
                }
        if revert:
            for collection in self.collections.itervalues():
                for gob in self.object.itervalues():
                    gob.revert()


    def upload(self, gob, fp):
        """Upload the data to the gob.

        Returns nothing, but sets the fields on the object if they are
        updated.
        """
        gob2 = self.storage_engine.upload(gob, fp)
        self._update_object(gob, gob2, True)


    def upload_iter(self, gob, iterable):
        """Upload the data to the gob, iterable version.

        Returns nothing, but sets the fields on the object if they are
        updated.
        """
        gob2 = self.storage_engine.upload_iter(gob, fp)
        self._update_object(gob, gob2, True)


    def download(self, gob):
        """Download the file from the gob.

        Returns and iterable containing the data.
        """
        gob2, iterable = self.do_download(self.gob_to_pgob(gob))
        self._update_object(gob, gob2, True)
        return iterable



class Backend(GobTranslator):
    """Abstract class for session back ends."""

    def query(self, path=None, path_range=None, query=None, retrieve=None,
              order=None, offset=None, limit=None):
        """Perform a query against the database.

        Should return a list of gob objects."""
        raise NotImplementedError("Backend type '%s' does not implement" \
                                      " query" % self.__class__.__name__)


    def commit(self, additions=[], updates=[], removals=[],
               collection_additions=[], collection_removals=[]):
        """Atomically commit some changeset to the db.

        The arguments additions, updates, and removals are
        dictionaries with the following keys:
        
        * gob -- the gob to perform the action on
        
        * condition -- a query to be run against the gob as it exists
          in the db to determine whether to perform the action or not.
          Absent for add.

        * add_keys -- additional keys under which the object
          should be stored.

        * add_unique_keys -- additional unique keys under which
          the object should be stored.

        * remove_keys -- additional keys from which the object should
          be removed.

        * remove_unique_keys -- additional unique keys which should be
          remoived.

        key_additions and key_removals should be lists of paths to
        collection keys to add empty or to remove entirely.
        """
        raise NotImplementedError("Backend type '%s' does not implement" \
                                      " commit" % self.__class__.__name__)



class StorageEngine(GobTranslator):
    """Abstract class for storage engines."""

    def upload(gob, fp):
        """Storage engines should provide this method.

        TODO: Provide a default version that wraps the file in an
        iterable object.

        Should return a gob representing the new / changed
        object.
        """
        raise NotImplementedError("Storage engine type '%s' does not" \
                                      " implement upload" \
                                      % self.__class__.__name__)


    def download(gob, fp):
        """Storage engines should provide this method.

        Should return a tuple of the updated gob and an iterable
        yielding the data.
        """
        raise NotImplementedError("Storage engine type '%s' does not" \
                                      " implement download" \
                                      % self.__class__.__name__)


    def upload_iter(gob, iterable):
        """Storage engines should provide this method.

        Should return a gob representing the new / changed object.
        The default version merely wraps the iterator as a file-like
        object and sends it to upload.
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
        return upload(gob, filewrapper())
