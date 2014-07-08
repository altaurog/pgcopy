pgcopy
=================

pgcopy is a small system for very fast bulk insertion of data into a
PostgreSQL database table using `binary copy`_.

Installation
-------------

pgcopy requires pytz_ and the psycopg2_ db adapter.
nose_ is required to run the tests.

Basic use
---------

pgcopy provides facility for copying data from an iterable of tuple-like
objects using a ``CopyManager``, which must be instantiated with a psycopg2
db connection, the table name, and an iterable indicating the names of the
columns to be inserted in the order in which they will be provided.
pgcopy inspects the database to determine the datatypes of the columns.

For example::

    from datetime import datetime
    from pgcopy import CopyManager
    import psycopg2
    cols = ('id', 'timestamp', 'location', 'temperature')
    now = datetime.now()
    records = [
            (0, now, 'Jerusalem', 72.2),
            (0, now, 'New York', 75.6),
            (0, now, 'Moscow', 54.3),
        ]
    conn = psycopg2.connect(database='weather')
    mgr = CopyManager(conn, 'measurements', cols)
    mgr.copy(records)

By default, a temporary file on disk is used.  If there's enough memory,
you can get a slight performance benefit with in-memory storage::

    from cStringIO import StringIO
    mgr.copy(records, StringIO)

Replacing a Table
------------------

When possible, faster insertion may be realized by inserting into an empty
table with no indices or constraints.  In a case where the entire contents
of the table can be reinserted, the ``Replace`` class automates the
creation of a new table, the recreation of constraints, indices, and
triggers, and the replacement of the old table with the new::

    from pgcopy import CopyManager, Replace
    with Replace(conn, 'mytable') as temp_name:
        mgr = CopyManager(conn, temp_name, cols)
        mgr.copy(records)


Supported datatypes
-------------------

Currently the following PostgreSQL datatypes are supported:

* bool
* smallint
* integer
* bigint
* real
* double precision
* char
* varchar
* text
* bytea
* date
* timestamp
* timestamp with time zone

Benchmarks
-----------

Below are simple benchmarks for 100000 records.
This gives a general idea of the kind of speedup 
available with pgcopy::

    $ nosetests -c tests/benchmark.cfg 
              ExecuteManyBenchmark:   7.75s
                   PGCopyBenchmark:   0.54s
    ----------------------------------------------------------------------
    Ran 2 tests in 9.101s

.. _binary copy: http://www.postgresql.org/docs/9.3/static/sql-copy.html
.. _psycopg2: https://pypi.python.org/pypi/psycopg2/
.. _pytz: https://pypi.python.org/pypi/pytz/
.. _nose: https://pypi.python.org/pypi/nose/
