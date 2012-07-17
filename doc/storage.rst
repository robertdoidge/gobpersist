Large Data and Storage Engines
==============================

.. currentmodule:: gobpersist.storage.Storable

When the size of data reaches a certain threshold, databases are no
longer the most efficient storage retrieval system.  To account for
this, Gobpersist offers the optional addition of associating a data
stream with a gob.  One can think of this as vaguely anlogous to the
"data" (as opposed to "resource") fork in the HFS family of file
systems.

To get a particular gob to work with a storage area, define the fields
``size`` and ``mime_type`` for that gob, and have it also inherit from
:class:`gobpersist.storage.Storable`.  For example::

   class WebstoreItem(gobpersist.gob.Gob, gobpersist.storable.Storable):
       """An item being offered for sale in our web store."""
       id = gobpersist.field.UUIDField(primary_key=True, unique=True)
       price = gobpersist.field.RealField()
       name = gobpersist.field.StringField(encoding="UTF-8")
       quantity_remaining = gobpersist.field.IntegerField()

       size = gobpersist.field.IntegerField()
       mime_type = gobpersist.field.StringField(encoding="UTF-8")

You can now call the methods :meth:`upload` (or :meth:`upload_iter`)
and :meth:`download` on the gob in order to set or access the data
stream for that gob.  These have corresponding methods in
:class:`Session <gobpersist.session.Session>`.

If you wish to have the md5sum of the data recorded as it is
streaming, define a md5sum :class:`String <gobpersist.string.String>`
field and inherit from :class:`MD5Storable
<gobpersist.storage.MD5Storable>` instead of :class:`Storable
<gobpersist.storage.Storable>`.

Note that currently there are no back end storage engines provided
with Gobpersist.  (At Accellion, we have an internal, specific storage
engine that it wouldn't make sense to release.)  It should be trivial
to define a generic file storage engine, and hopefully this will soon
exist in gobpersist.  See :ref:`todo`.
