Introduction
============

.. include:: ../README
    :end-before: All documentation

Background and rationale
------------------------

In the early days of computing, data storage was a disorganized mess.
There was no common method for representing the connections between
each individual datum, and so you had a plethora of mutually exclusive
models, not interoperable and difficult to translate between.  The
data storage model was therefore tightly embedded with the
application.

In 1970, Codd published "A Relational Model of Data for Large Shared
Data Banks," insisting that data be represented as atomic tuples, with
no differentiation between key and data, and a separation of concerns
between the interface to and the actual storage of data.  SQL
(Structured Query Language), following shortly thereafter, offered a
universal interface to databases following the relational model.  For
a while, then, we had a de facto standard data model based on a
monolithic data server that knows everything about how data is stored
but nothing about what to do with it, and a client that knows
everything about what to do with the data but nothing about how it is
stored.  ACID (Atomicity, Consistency, Isolation, Durability) became
the standard set of attributes for these databases.

As parallel scalability became more important during the early 2000s,
data servers quickly became the single piece of the system that could
not be easily scaled.  Between 2000 and 2002, Eric Brewer and Seth
Gilbert came up with the CAP theorem for databases, stating that: of
**C**\ onsistency, **A**\ vailability, and **P**\ artition tolerance,
a distributed system could only ever guarantee two of these at one
time.  As ACID SQL databases implicitly or explicitly guaranteed
consistency, one of availability or partition tolerance had to be
given up.  The response has been, and largely still is, to give up
partition tolerance---that is, to maintain synchronously maintained
consistency between parallel data sets.  As the overhead for this
grows with the square of network size, this severely limits the
possibilities for SQL.  The standard has been a single write server,
mirrored by a slave server that will take over in case of failure,
with an array of read-only servers in front.

NoSQL---and consequently Gobpersist, as well---addresses this
scalability problem in two ways.  First, by exposing the internals of
how data is stored---by reversing the separation of concerns put forth
by the relational database model---NoSQL data stores allow programs to
be built that can tolerate a certain kind of potential inconsistency,
thus allowing fully parallel, available, and partition tolerant stores
to be built.  Second, by simplifying the design of the database
engine, NoSQL stores have been able to scale much better even on a
single machine.

But now databases have become a mess again.  Gobpersist wants to be
the best of both worlds, to achieve both kinds of NoSQL stability
without giving up on what was valuable in SQL.  Its central mission is
therefore to create a database middle layer that abstracts the
interface from the specific database implementation, allows the
programmer enough control over *how* queries are being made to allow
for controllable inconsistency, and keeps the back end requirements
simple enough that virtually any database can be used with Gobpersist
(hence *Generic* Object Persistence).

Design principles
-----------------

Gobpersist has been developed according to the following design
principles.  Keep in mind that these are all ideals in whose
achievement the code was sometimes more, sometimes less successful.

* **Scalability, not speed.**

  Build the code assuming many machines will run it in parallel.  When
  an algorithm can be implemented at greater expense, but it takes
  load off of the database, implement the more expensive version.

* **Be ambiguous, don't deduce an interface from first principles.**

  Actual practices of data representation in computing have settled,
  more or less, on a very small set of possible entities: scalar
  values, lists, dictionaries, and references.  Rather than normalize
  this data according to strict definitions, Gobpersist assumes that
  these entities have whatever semantics they have in whatever
  database or data encoding system the back end uses (think of C's
  ``int``).  In Gobpersist, these entities correspond to `fields`,
  :`collections`, `gobs` and `foreign fields`.  (See :ref:`schema` for
  more.)

* **Queries are data.  Querying should accord to data, not data to
  querying.**

  Gobpersist uses a :ref:`query syntax <query>` made up of python
  lists, tuples, dictionaries, and scalars.  These queries should be
  equivalent in power to a SQL select statement.  Since queries are
  already data, creating a named-query system should be trivial (but
  this has not yet been done; :ref:`todo`).

* **Clarity of expression, not economy of expression.**

  If databases must be rigorously and carefully defined, then the code
  defining them must be eminently readable.  Redundancy is good when
  it makes things more apparent to the person reading the code.
  Quantum physics may be the theoretical basis for our understanding
  of the world, but it is to be avoided in code: semantic effects from
  nonlocal code are to be avoided.

* **Less magic.**

  Though this is just a corollary of the last principle, it gets its
  own mention since this is contrary to most existing ORM designs.
  There is magic here and there, where it really makes sense, but in
  general the programmer should understand more or less exactly what
  happens when she writes a line of code.

Development status
------------------

Gobpersist is fully functional, and parts of it have been tested
extensively.  However, it is not yet functionally complete, and it is
likely that there is some code that has never been run, and some code
that was only correct within the limited context in which it has been
tested.  That's where you come in: help us break, and then fix,
gobpersist in new exciting ways.

Currently, Gobpersist has two back ends available: `Tokyo Tyrant
<http://fallabs.com/tokyotyrant/>`_ and `Memcached
<http://memcached.org/>`_ (or anything that uses the Memcached
protocol).  We will gladly accept code for more back ends.  It should
be relatively easy.  The Tokyo Tyrant back end is less than 500 lines
of code.

Mailing list
------------

To develop gobpersist, or to get more help, join the gobpersist
mailing list, or read the archives, at
http://groups.google.com/group/gobpersist.

License
-------

Gobpersist is released under the LGPLv2.1 license.  If this doesn't
suit your needs, contact open-source@accellion.com.

Credits
-------

Gobpersist was developed by `Accellion <http://www.accellion.com>`_ to
catapult its next generation of appliances into the future.  Accellion
will continue to actively develop Gobpersist in the foreseeable
future, pushing the envelope to achieve an innovative, stable,
scalable, secure---and hopefully, pretty---system.

Email open-source@accellion.com for more information about open source
at Accellion.

The current maintainer is Evan Buswell <evan.buswell@accellion.com>.
He designed gobpersist and wrote these docs and most of the code.
