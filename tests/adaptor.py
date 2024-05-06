import importlib
import sys

import pytest


class Psycopg2:
    def __init__(self, connection_params, client_encoding):
        try:
            psycopg2 = importlib.import_module("psycopg2")
            extras = importlib.import_module("psycopg2.extras")
        except ModuleNotFoundError:
            pytest.skip("psycopg2 not available")

        self.conn = psycopg2.connect(
            connection_factory=extras.LoggingConnection,
            **connection_params,
        )
        self.conn.initialize(sys.stderr)
        self.conn.autocommit = False
        self.conn.set_client_encoding(client_encoding)
        self.errors = psycopg2.errors


class Psycopg3:
    def __init__(self, connection_params, client_encoding):
        try:
            psycopg3 = importlib.import_module("psycopg")
        except ModuleNotFoundError:
            pytest.skip("psycopg3 not available")

        self.conn = psycopg3.connect(**connection_params)
        self.conn.autocommit = False
        self.conn.execute(f"SET client_encoding='{client_encoding}'")
        self.errors = psycopg3.errors
