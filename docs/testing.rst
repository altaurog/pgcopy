Testing
--------

For a fast test run using the available python, use pytest_::

    $ pytest tests

The test suite uses the following environment variables to connect to the
database:

* ``POSTGRES_DB`` (default ``pgcopy_test``)
* ``POSTGRES_HOST``
* ``POSTGRES_PORT`` (default ``5432``)
* ``POSTGRES_USER``
* ``POSTGRES_PASSWORD``


One of psycopg2_ or psycopg_ is required to run the tests.  The test suite
will automatically discover all supported db adaptors.

For more thorough testing, tox_ with tox-docker_ will run tests on python
versions 3.9 -- 3.14 and postgresql versions 13 -- 18::

    $ tox

Additionally, the test suite can be run with no local requirements other
than the ubiquitous docker::

    $ docker-compose up pgcopy

The pgcopy test suite can also be run against AWS Aurora DSQL.  For this,
boto3 must be installed and ``POSTGRES_HOST`` set to the dsql endpoint.

.. note::

    Tests for extension types will be skipped if the required db extension is not
    available.


.. _pytest: https://pypi.org/project/pytest/
.. _tox: https://tox.wiki
.. _tox-docker: https://tox-docker.readthedocs.io
.. _psycopg2: https://pypi.org/project/psycopg2/
.. _psycopg: https://pypi.org/project/psycopg/
