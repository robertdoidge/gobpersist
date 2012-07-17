.. _schema:

Defining a Database Schema
==========================

Gobpersist more or less follows the *active record* design pattern.
That is, the classes that represent your data have behavior and
semantics specific to Gobpersist, including schema enforcement at the
code level.  Since Gobpersist is designed to take over functionality
traditionally assigned to the database, these classes are not just a
definition of maps to data, but an important part of the database
system itself.

Gobpersist uses three classes to define schemas: :class:`Gob
<gobpersist.gob.Gob>`, :class:`Field <gobpersist.field.Field>`, and
:class:`Schema <gobpersist.schema.Schema>`.

Overview
--------

Gobs are what is stored in gobpersist.  They consist of some number of
named fields, as well as key and storage information.  They are
roughly equivalent to a SQL table definition.

To define a new kind of gob, just create a class that inherits from
:class:`Gob <gobpersist.gob.Gob>`, that has a number of :class:`Field
<gobpersist.field.Field>` objects defined:

.. _example_schema:

::

   class WebstoreItem(gobpersist.gob.Gob):
       """An item being offered for sale in our web store."""
       id = gobpersist.field.UUIDField(primary_key=True, unique=True)
       price = gobpersist.field.RealField()
       name = gobpersist.field.StringField(encoding="UTF-8")
       quantity_remaining = gobpersist.field.IntegerField()

   class WebstoreUser(gobpersist.gob.Gob):
       """A user of our web store."""
       email = gobpersist.field.StringField(
           unique=True, primary_key=True, encoding="UTF-8",
	   validate=lambda x: re.match("[^@]@[^@].org", x) is not None)
       creditcard = gobpersist.field.StringField(encoding="US-ASCII")
       address = gobpersist.field.StringField(encoding="UTF-8")

Note that most back ends will depend on you defining a primary key of
some sort for every gob.

Although gobpersist can technically be used without it, it's easier if
you define a schema to contain all of your gobs. ::

   class WebstoreSchema(gobpersist.schema.Schema):
       items = WebstoreItem
       users = WebstoreUsers

This is a complete, working gobpersist schema, ready to be initialized
in the database.

Fields
------

Fields are scalar data items present within a gob.  The following
field types are available:

.. currentmodule:: gobpersist.field

* :class:`BooleanField` -- A single boolean value.
* :class:`DateTimeField` -- A date/time.
* :class:`TimestampField` -- A date/time automatically set to the last edit.
* :class:`StringField` -- A string of binary or character data.
* :class:`IntegerField` -- An integer.
* :class:`IncrementingField` -- An integer that automatically
  increments on each edit.
* :class:`RealField` -- A floating-point number.
* :class:`EnumField` -- An enumeration of several string values.
* :class:`UUIDField` -- A `UUID
  <https://en.wikipedia.org/wiki/Universally_unique_identifier>`_,
  version 4 (random) by default.
* :class:`ListField` -- A uniformly-typed list of values, where the
  type is some other field.  Lists may be nested.
* :class:`SetField` -- A uniformly-typed set of values, where the type
  is some other field.  Sets may be nested.

Note that :class:`ListField` and :class:`SetField` are not exactly
scalar data in the traditional sense of the word, but they are opaque
to many database operations.

The current value of a field is always stored in :attr:`Field.value`,
but so long as your program (or the libraries with which your program
interacts) properly uses duck typing, you should never need to know
that.  Gobpersist fields override enough methods that each instance of
a specific field should act just like the object it contains.
Additionally, Gobs have the appropriate magic so that setting a field
to its value directly just works.

So for example, with the :ref:`schema above <example_schema>`, you
should be able to do the following::

   >>> myitem = WebstoreItem()
   >>> myitem.name = 'Defense Technology MK-9 Stream Pepper Spray'
   >>> myitem.name
   'Defense Technology MK-9 Stream Pepper Spray'
   >>> len(myitem)
   43
   >>> myitem.index('Pepper')
   31

If the provided types don't suit your needs, it's not too difficult to
define your own.  Start by subclassing a type that's close to your
needs, and then override :meth:`Field.validate` and whatever methods
necessary to give your field the desired behavior.

Keys and how gobs are stored
----------------------------

.. currentmodule:: gobpersist.gob.Gob

In Gobpersist, all database keys are tuples.  Each gob has a
:attr:`class_key` attribute, which indicates the first element of the
primary key---the key name.  By default :attr:`class_key` is set to
the lowercase name of the class, plus an "s."  For the :ref:`above
example <example_schema>`, the :attr:`class_key` would be set to
"webstoreitems" and "webstoreusers," respectively.

The gob is always stored under the :attr:`obj_key` for the class,
which is by default ``(class_key, primary_key)`` (:attr:`primary_key`
is always an alias for the field that is the primary key for the
class).  You can create any arbitrary combination of other keys to aid
queries on the object.  The relevant class attributes are :attr:`keys`
and :attr:`unique_keys`.  A `key` indicates a collection of gobs,
whereas a `unique key` always refers to exactly one gob.

Let's, for example, imagine that, due to widespread credit card
default, our web store gets a direct event feed from credit card
companies indicating credit card numbers that are no longer valid.
We'd then want to have a credit card key for each user.  If we wanted
to keep two users from using the same credit card for some reason, we
would make this a unique key.  But probably we'd probably allow
multiple users to have the same credit card, and put this in
:attr:`keys`. ::

   class WebstoreUser(gobpersist.gob.Gob):
       """A user of our web store."""
       email = gobpersist.field.StringField(
           unique=True, primary_key=True, encoding="UTF-8",
	   validate=lambda x: re.match("[^@]@[^@].org", x) is not None)
       creditcard = gobpersist.field.StringField(encoding="US-ASCII")
       address = gobpersist.field.StringField(encoding="UTF-8")

       keys = [('webstoreusers-by-creditcard', creditcard)]

Note that "webstoreusers-by-creditcard" is just an arbitrary string.

If instead of a credit card default stream, we had a stream of changed
addresses on credit cards, we'd need a key that used both the credit
card and the address.  We'd define this like::

   class WebstoreUser(gobpersist.gob.Gob):
       """A user of our web store."""
       email = gobpersist.field.StringField(
           unique=True, primary_key=True, encoding="UTF-8",
	   validate=lambda x: re.match("[^@]@[^@].org", x) is not None)
       creditcard = gobpersist.field.StringField(encoding="US-ASCII")
       address = gobpersist.field.StringField(encoding="UTF-8")

       keys = [('webstoreusers-by-creditcard-address', creditcard, address)]

For some back ends, order is not significant.  But for many, these
keys are implemented as a linear combination of all the terms, which
means that only the last term can really be used in a key range query.
See :ref:`query` for more information on how queries are made.

In addition to the :attr:`obj_key`, all gobs automatically define
:attr:`coll_key`, the key for the collection the Gob represents.  By
default, this is ``(class_key,)``, and nothing is stored under it.
You have to explicitly tell gobpersist when to store something under
the collection key, by adding it to the :attr:`keys` list or through
the :meth:`keyset` method (see below)---or by setting :attr:`coll_key`
to the value of an existing key.  The collection key is usually
relevant for holding the root of hierarchical data for use with
:meth:`SchemaCollection.list
<gobpersist.schema.SchemaCollection.list>`, or for holding a reference
to all gobs of a particular kind for queries that don't involve an
explicit key.

Sometimes the rules for determining what key an object is stored under
are more complex.  For that situation, you can override the methods
:meth:`keyset` or :meth:`unique_keyset`.  For example, if we wanted to
have the most expensive item in our web store readily accessible, the
following code would accomplish this, assuming you had the price of
the most expensive item cached as ``spendy``::

   class WebstoreItem(gobpersist.gob.Gob):
       """An item being offered for sale in our web store."""
       id = gobpersist.field.UUIDField(primary_key=True, unique=True)
       price = gobpersist.field.RealField()
       name = gobpersist.field.StringField(encoding="UTF-8")
       quantity_remaining = gobpersist.field.IntegerField()

       def unique_keyset(self):
           if self.price >= spendy:
               spendy = self.price.clone(clean_break=True)
               return self.unique_keys + [('spendiest',)]
           else:
               return self.unique_keys

Foreign references
------------------

Because of the design constraint to keep nonlocal references out of
Gobpersist, foreign references can involve more redundancy than for
other ORM systems.  The payoff of this redundancy, however, is that,
by reading any single gob definition, one should be able to understand
exactly what kind of references either to or from that gob need to be
taken into account.

.. currentmodule:: gobpersist.field

The basis of foreign references in gobpersist are the
:class:`ForeignObject` and :class:`ForeignCollection` classes, for
referencing a single object or a collection of objects, respectively.
Unlike conventional ORMs, which declare references on the fields doing
the referencing, Gobpersist keeps :class:`ForeignObject` and
:class:`ForeignCollection` declarations separate from the declaration
of the field being used as a reference.  This allows for them to be
used with arbitrarily complex keys.

For our example, let's suppose each user can choose a favorite item.

::

   class WebstoreItem(gobpersist.gob.Gob):
       """An item being offered for sale in our web store."""
       id = gobpersist.field.UUIDField(primary_key=True, unique=True)
       price = gobpersist.field.RealField()
       name = gobpersist.field.StringField(encoding="UTF-8")
       quantity_remaining = gobpersist.field.IntegerField()

       def unique_keyset(self):
           if self.price >= spendy:
               spendy = self.price.clone(clean_break=True)
               return self.unique_keys + [('spendiest',)]
           else:
               return self.unique_keys

   class WebstoreUser(gobpersist.gob.Gob):
       """A user of our web store."""
       email = gobpersist.field.StringField(
           unique=True, primary_key=True, encoding="UTF-8",
	   validate=lambda x: re.match("[^@]@[^@].org", x) is not None)
       creditcard = gobpersist.field.StringField(encoding="US-ASCII")
       address = gobpersist.field.StringField(encoding="UTF-8")
       favorite_item_id = gobpersist.field.UUIDField(primary_key=True, null=True, default=None)

       keys = [('webstoreusers-by-creditcard-address', creditcard, address),
               ('webstoreusers-by-favorite-item', favorite_item_id)]

       favorite_item = ForeignObject(foreign_class=WebstoreItem,
                                     local_field='favorite_item_id',
                                     foreign_field='id',
                                     key=('webstoreitems', favorite_item_id))


If you wanted to add a reverse reference from the item to the user who
favorited it, things get a bit tricky.  Since :class:`WebstoreUser`
doesn't exist at the time :class:`WebstoreItem` is being defined, you
have to add on to the class later, then reload the class using
:meth:`reload_class <gobpersist.gob.Gob.reload_class>`.  As a special
case, the vale ``'self'`` will be replaced with the class in which it
is defined.  Here's the full example::

   class WebstoreItem(gobpersist.gob.Gob):
       """An item being offered for sale in our web store."""
       id = gobpersist.field.UUIDField(primary_key=True, unique=True)
       price = gobpersist.field.RealField()
       name = gobpersist.field.StringField(encoding="UTF-8")
       quantity_remaining = gobpersist.field.IntegerField()

       def unique_keyset(self):
           if self.price >= spendy:
               spendy = self.price.clone(clean_break=True)
               return self.unique_keys + [('spendiest',)]
           else:
               return self.unique_keys

   class WebstoreUser(gobpersist.gob.Gob):
       """A user of our web store."""
       email = gobpersist.field.StringField(
           unique=True, primary_key=True, encoding="UTF-8",
	   validate=lambda x: re.match("[^@]@[^@].org", x) is not None)
       creditcard = gobpersist.field.StringField(encoding="US-ASCII")
       address = gobpersist.field.StringField(encoding="UTF-8")
       favorite_item_id = gobpersist.field.UUIDField(primary_key=True, null=True, default=None)

       keys = [('webstoreusers-by-creditcard-address', creditcard, address),
               ('webstoreusers-by-favorite-item', favorite_item_id)]

       favorite_item = ForeignObject(foreign_class=WebstoreItem,
                                     local_field='favorite_item_id',
                                     foreign_field='id',
                                     key=('webstoreitems', favorite_item_id))

   WebstoreItem.users_favorited = ForeignCollection(
       foreign_class=WebstoreUser,
       local_field='id',
       foreign_field='foreign_item_id',
       key=('webstoreusers-by-favorite-item', WebstoreItem.id))
   WebstoreItem.reload_class()

Currently, the only way to create a many-to-many relationship is to
create a Gob that functions like a pivot table.  Some of this is a
little clunky, and may be improved in future versions of Gobpersist.

Cascading updates
-----------------

.. currentmodule:: gobpersist.gob.Gob

Foreign fields, however, are not just about access.  Gobpersist also
provides a mechanism to make sure that consistency is maintained
across different objects: :attr:`consistency` and
:attr:`set_consistency`, for objects and collections, respectively.
These attributes specify what effect adding objects, deleting objects,
or changing certain fields on an object will have.

Using our simple web store example, what happens when we delete a
:class:`WebstoreItem` that was a user's favorite?  Ideally, we would
want to return that user's favorite_item_id to the unset state, that
is, ``None``.  Here's how we would accomplish that::

   class WebstoreItem(gobpersist.gob.Gob):
       """An item being offered for sale in our web store."""
       id = gobpersist.field.UUIDField(primary_key=True, unique=True)
       price = gobpersist.field.RealField()
       name = gobpersist.field.StringField(encoding="UTF-8")
       quantity_remaining = gobpersist.field.IntegerField()

       def unique_keyset(self):
           if self.price >= spendy:
               spendy = self.price.clone(clean_break=True)
               return self.unique_keys + [('spendiest',)]
           else:
               return self.unique_keys

   class WebstoreUser(gobpersist.gob.Gob):
       """A user of our web store."""
       email = gobpersist.field.StringField(
           unique=True, primary_key=True, encoding="UTF-8",
	   validate=lambda x: re.match("[^@]@[^@].org", x) is not None)
       creditcard = gobpersist.field.StringField(encoding="US-ASCII")
       address = gobpersist.field.StringField(encoding="UTF-8")
       favorite_item_id = gobpersist.field.UUIDField(primary_key=True, null=True, default=None)

       keys = [('webstoreusers-by-creditcard-address', creditcard, address),
               ('webstoreusers-by-favorite-item', favorite_item_id)]

       favorite_item = ForeignObject(foreign_class=WebstoreItem,
                                     local_field='favorite_item_id',
                                     foreign_field='id',
                                     key=('webstoreitems', favorite_item_id))

   WebstoreItem.users_favorited = ForeignCollection(
       foreign_class=WebstoreUser,
       local_field='id',
       foreign_field='foreign_item_id',
       key=('webstoreusers-by-favorite-item', WebstoreItem.id))
   WebstoreItem.consistency=[{
       'field': WebstoreItem.id,
       'foreign_class': WebstoreUser,
       'foreign_obj': ('webstoreusers-by-favorite-item', WebstoreItem.id),
       'foreign_field': 'favorite_item_id',
       'update': 'cascade',
       'remove': 'set_default',
       'invalidate': 'cascade'}]
   WebstoreItem.reload_class()

In this case, we don't add a similar consistency field to
:class:`WebstoreUser`, as changes to :attr:`favorite_item_id` imply
that the implied item is simply no longer referenced by that user.
