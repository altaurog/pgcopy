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

There is also an optimized ``CopyManager`` written in Cython_ for inserting
data from a pandas_ DataFrame::

    import numpy as np
    import pandas as pd
    import psycopg2
    from pgcopy import ccopy
    cols = ('a', 'b', 'c')
    df = pd.DataFrame(np.random.randn(500, 3), columns=cols)
    conn = psycopg2.connect(database='weather')
    mgr = CopyManager(conn, 'measurements', cols)
    mgr.copy(df)

``text`` and ``bytea`` types are not supported with the optimized backend.

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

Below are simple benchmarks for 100000 records::

    $ nosetests -c tests/benchmark.cfg 
    ----------------------------------------------------------------------
    Benchmark execution times:
                        copystream: 0.08s
                       writestream: 0.98s
    ----------------------------------------------------------------------
    CythonBenchmark execution times:
                        copystream: 0.08s
                       writestream: 0.37s
    ----------------------------------------------------------------------
    ExecuteManyBenchmark execution times:
                       executemany: 15.76s
    ----------------------------------------------------------------------
    NullBenchmark execution times:
                        copystream: 0.08s
                       writestream: 1.04s
    ----------------------------------------------------------------------
    NullCythonBenchmark execution times:
                        copystream: 0.08s
                       writestream: 0.42s
    ----------------------------------------------------------------------
    NullExecuteManyBenchmark execution times:
                       executemany: 15.79s
    ----------------------------------------------------------------------
    Ran 6 tests in 38.473s



.. _binary copy: http://www.postgresql.org/docs/9.3/static/sql-copy.html
.. _psycopg2: https://pypi.python.org/pypi/psycopg2/
.. _pytz: https://pypi.python.org/pypi/pytz/
.. _nose: https://pypi.python.org/pypi/nose/
