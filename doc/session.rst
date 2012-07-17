Using Gobpersist
================

Gobpersist communicates to the database via :class:`sessions
<gobpersist.session.Session>`.  Every session has a back end, which
does most of the actual work in storing data.  Sessions are proxy
objects that collect data and translate code from that easiest for the
programmer to interact with to that easiest for the back end developer
to implement.

To create a session, create a new session object, passing in (or also
creating) a back end. ::

   session = gobpersist.session.Session(
       backend=gobpersist.backends.tokyotyrant.TokyoTyrantBackend(
           host='192.168.161.80',
	   serializer=json
       )
   )

The session object then can be used to create, update, remove, and
query the database for gobs.

Using a gob
-----------

When you create a :ref:`gob <schema>` object, you pass in the session.
The gob then knows how to notify the session of additions, updates and
removals.

* To add a gob::

   sgc1 = SomeGobClass(session=session)
   sgc1.save()

* To update a gob::

   sgc1.somefield = "New Value"
   sgc1.save()

* To delete a gob::

   sgc1.remove()

* To revert a gob to its saved, database version::

   sgc1.revert()

Using a schema
--------------

.. currentmodule:: gobpersist.schema

Sessions are most often used by :class:`Schema` objects, which provide
a simpler, but more simplistic, interface than :class:`Session
<gobpersist.session.Session>` objects.  For more on schemas, see
:ref:`schema`.

To create a schema, you must first create a session.  You create the
schema passing in the session objects through which the schema will
communicate with the back end. ::

   myschema = WebstoreSchema(session=session)

Before you use a schema for the first time, it must be initialized.
Missing keys will automatically be created by most back ends, but
certain collections should be present, but empty, by default, rather
than simply not present.::

   myschema.initialize_db()

Note that this is still very primitive and will happily wipe out your
existing data, so only use it with truly empty collections.  (There is
not yet a simple way to alter your database schema, but it can be done
manually with :meth:`Session.add_collection
<gobpersist.session.Session.add_collection>`.  (See :ref:`todo`.)

The schema can then be explored through its :class:`SchemaCollection`
objects, each of which are defined to correspond to the gobs defined
within this schema.  For example, to list all users, one would write::

   myschema.users.list()

Note that this actually lists only the users stored under the
:attr:`coll_key <gobpersist.gob.Gob.coll_key>` for the gob, which you
may or may not have stored anything under.  To start with a different
key, use :meth:`Schema.collection_for_key`.

Note that instances of :class:`ForeignCollection
<gobpersist.field.ForeignCollection>` function just like a
:class:`SchemaCollection`, such that hierarchical listing is very
easy. ::

   for parent in myschema.users.list():
       for child in parent.children.list():
           # do something useful
           pass

:meth:`list <SchemaCollection.list>` has a convenience query language
that is much simpler than the :ref:`full query language <query>`.  For
:meth:`list <SchemaCollection.list>`, you simply pass in key-value
pairs of attribute names and the values to which they must be equal.
You can use comparators other than ``'eq'`` by using tuples, for
example::

   myschema.list(name='abc123', num_users=('gt', 20))

If you want to use the full query language, instead pass a query in
the `_query` parameter.

To add a gob directly to a collection---that is, to set any values
such that the gob will show up in the current collection, use
:meth:`add <SchemaCollection.add>`::

   myschema.webstoreitems.add(sgc1)

Gobpersist, however, is not quite able to ascertain which values need
to be set in all cases.  To simplify the process, you must define it
yourself via the `autoset` parameter.  :class:`ForeignCollection
<gobpersist.field.ForeignCollection>` fields will set this for you
automatically, but by default---for SchemaCollection objects created
manually or through a Schema object---`autoset` is blank.  If you need
to explicitly set a value, you should set `autoset` on the collection.

* To delete a gob::

   myschema.webstoreitems.remove(sgc1)

* To update a gob::

   myschema.webstoreitems.update(sgc1)

Sessions, atomicity, and commits
--------------------------------

.. currentmodule:: gobpersist.session

All activity in Gobpersist takes place via a :class:`Session`.  A
session is a single thread of activity in the database---that is,
sessions are *not* thread safe.

All the above methods eventually call :meth:`Session.add`,
:meth:`Session.remove`, or :meth:`Session.update`.  To add a
collection key without adding a gob---or to remove an unused
collection key---you can call :meth:`add_collection` or
:meth:`remove_collection`.  :meth:`Schema.initialize_db
<gobpersist.schema.Schema.initialize_bd>` will call this method for
all relevant collections, but currently any changes to the present
collections other than initialization must call these methods
directly.

Gobpersist is a transaction-based system.  That means that once
committed, a transaction succeeds or fails as a whole, it is
atomic---although particular back ends may override that in
well-defined ways.  All of the above commands, then, merely schedule a
change to be committed.  To commit a session, call
:meth:`Session.commit`.  When one queries an object that has already
been altered in the current session, Gobpersist has weakly consistent
database views through deduplication.  For a given Gob, all gobs with
the same primary key will be represented by the same Python object.
Although this allows changes to be viewed on all copies of the object,
inclusion or exclusion of gobs in certain queries may still represent
the previous database state, depending on the back end.  This may be
fixed in the future.

To roll back a transaction, use :meth:`Session.rollback`---by default
this will leave all changes intact in the objects themselves, but
merely cancel all pending operations on those obejcts.  However,
optionally, you can tell :meth:`rollback` to revert all extant objects
in the transaction to their previous state.  Gobpersist has minimal
support for nested transactions.  The option to revert the changes of
individual fields is not aware of nested transactions; only the
pending operations will be discarded or committed to the previous
transaction.

The whole transaction system is very minimal right now, and you must
program with that awareness in mind.  However, it is also already
quite useful, and most code written for this minimal transaction
system will be the same if and when Gobpersist supports full
transactions.  See :ref:`todo`.

To further support atomicity, gobpersist offers the concept of a
*revision tag*.  For a commit to succeed, the revision tags of all
objects must match what is currently found in the database.  This
provides support for building a distributed system that relies on
`Version vectors <https://en.wikipedia.org/wiki/Version_vector>`_ for
consistency.  To designate a field as a revision tag, simply set the
parameter `revision_tag` to ``True`` when creating the field object
during gob definition.
