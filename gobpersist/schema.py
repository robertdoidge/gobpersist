# Moved to end to avoid mutual dependency
# import gobpersist.gob

class SchemaCollection(object):
    def __init__(self, session, path, sticky=None):
        self.session = session
        self.path = path
        self.sticky = sticky

    def _translate_qelem(self, k, v):
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
        ret = {'and' : []}
        for k, v in query:
            ret['and'].append(self._translate_qelem(k, v))
        return ret


    def _translate_query(self, query):
        ret = _translate_and(self, query)
        if sticky is not None:
            ret['and'].append(sticky)
        return ret

    def list(self, _query=None, **kwargs):
        if _query is None:
            _query = self._translate_query(kwargs)
        return self.session.query(self.path, _query)

    def get(self, _query=None, **kwargs):
        res = self.session.query(self.path, _query, **kwargs)
        return res[0] if res else None

    def add(self, gob):
        gob.session = self.session
        gob.save()

    def update(self, gob=None, set=None, query=None, _query=None):
        if gob is not None:
            gob.save()
        if query is not None or _query is not None:
            if _query is None:
                _query = self._translate_query(kwargs)
            self.update_query(self.path, set=set, query=_query)

    def remove(self, gob=None, _query=None, **kwargs):
        if gob is not None:
            gob.remove()
        if _query is not None or kwargs:
            if _query is None:
                _query = self._translate_query(kwargs)
            session.remove_query(self.path, _query)

class Schema(object):
    def __init__(self, session):
        for name in dir(self):
            collection = getattr(self, name)
            if isinstance(collection, gob.Gob):
                setattr(self, name, SchemaCollection(session, (collection,)))
        self.session = session

    def __getattr__(self, name):
        return getattr(self.session, name)

import gobpersist.gob
