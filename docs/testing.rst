Testing
--------

For a fast test run using the available python, use pytest_::

    $ pytest tests

The test suite uses the following environment variables to connect to the
database:

* ``POSTGRES_DB`` (default ``pgcopy_test``)
* ``POSTGRES_HOST`` (default ``5432``)
* ``POSTGRES_PORT``
* ``POSTGRES_USER``
* ``POSTGRES_PASSWORD``


For more thorough testing, tox_ with tox-docker_ will run tests on python
versions 3.9 -- 3.14 and postgresql versions 13 -- 18::

    $ tox

Additionally, the test suite can be run with no local requirements other
than the ubiquitous docker::

    $ docker-compose up pgcopy

.. note::

    Tests for extension types will be skipped if the required db extension is not
    available.


.. _pytest: https://pypi.org/project/pytest/
.. _tox: https://tox.wiki
.. _tox-docker: https://tox-docker.readthedocs.io
