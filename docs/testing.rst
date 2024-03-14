Testing
--------

For a fast test run using current environment, use pytest_::

    $ pytest tests

For more thorough testing, Tox_ configuration will run tests on python
versions 2.7 and 3.6 -- 3.8::

    $ tox

Additionally, test can be run with no local requirements other than the
ubiquitous docker::

    $ docker-compose up pgcopy

.. note::

    Tests for extension types will be skipped if the extension is not
   available.


.. _pytest: https://pypi.org/project/pytest/
.. _Tox: https://tox.readthedocs.io/en/latest/
