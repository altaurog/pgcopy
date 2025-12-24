Database Adaptors
-----------------

As of version 2.0, pgcopy supports multiple db adaptors.
The db adaptor is automatically detected when a connection is passed to
:py:class:`pgcopy.CopyManager`.

=============== =================
DB Adaptor      Notes
=============== =================
psycopg2_       All features are supported
psycopg_        :any:`threading_copy` is the same as :any:`copy`.
                It does not actually use a thread, since psycopg copy is non-blocking.
pg8000_         UTF-8 encoding is strongly recommended on both server and client.
                :any:`threading_copy` is not supported.
PyGreSQL_       UTF-8 encoding is strongly recommended on both server and client.
                :any:`threading_copy` is not supported.
=============== =================


Tests
''''''
One of psycopg2_ or psycopg_ is required to run the tests.
The test suite will automatically discover all supported db adaptors.


.. _psycopg2: https://pypi.org/project/psycopg2/
.. _psycopg: https://pypi.org/project/psycopg/
.. _pg8000: https://pypi.org/project/pg8000/
.. _PyGreSQL: https://pypi.org/project/PyGreSQL/
