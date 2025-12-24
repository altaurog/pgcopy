import argparse
import codecs
import contextlib
import importlib
import os
import sys

from . import db_connection


def available_adaptors():
    adaptors = [Psycopg2, Psycopg3, PyGreSQL, Pg8000]
    return [a for a in adaptors if a.load()]


class Adaptor:
    module_names: list[str]
    pgcode_attribute = "sqlstate"
    m: argparse.Namespace

    @classmethod
    def load(cls):
        try:
            modules = [importlib.import_module(m) for m in cls.module_names]
            moddict = dict(zip(map(modname, cls.module_names), modules))
            cls.m = argparse.Namespace(**moddict)
            return True
        except ModuleNotFoundError:
            return False

    @classmethod
    def get_pgcode(cls, err):
        return getattr(err, cls.pgcode_attribute, None)


def modname(modpath):
    return modpath.rsplit(".", 1)[-1]


class Psycopg2(Adaptor):
    module_names = ["psycopg2", "psycopg2.extras"]
    pgcode_attribute = "pgcode"

    def __init__(self, connection_params, client_encoding):
        self.conn = self.m.psycopg2.connect(
            connection_factory=self.m.extras.LoggingConnection,
            **connection_params,
        )
        self.conn.initialize(sys.stderr)
        self.conn.autocommit = False
        self.conn.set_client_encoding(client_encoding)
        self.unsupported_type = self.m.psycopg2.errors.UndefinedObject
        self.integrity_error = self.m.psycopg2.errors.IntegrityError

    @staticmethod
    def supports_encoding(encoding):
        return True


class Psycopg3(Adaptor):
    module_names = ["psycopg"]

    def __init__(self, connection_params, client_encoding):
        self.conn = self.m.psycopg.connect(**connection_params)
        self.conn.autocommit = False
        self.conn.execute(f"SET client_encoding='{client_encoding}'")
        self.unsupported_type = self.m.psycopg.errors.UndefinedObject
        self.integrity_error = self.m.psycopg.errors.IntegrityError

    @staticmethod
    def supports_encoding(encoding):
        return True


class PyGreSQL(Adaptor):
    module_names = ["pgdb"]

    def __init__(self, connection_params, client_encoding):
        self.conn = self.m.pgdb.connect(**connection_params)
        self.conn.autocommit = False
        self.conn.execute(f"SET client_encoding='{client_encoding}'")
        self.unsupported_type = self.conn.ProgrammingError
        self.integrity_error = self.conn.IntegrityError

    @staticmethod
    def supports_encoding(encoding):
        try:
            codecs.lookup(encoding)
            return True
        except LookupError:
            return False


class Pg8000(Adaptor):
    module_names = ["pg8000.dbapi", "pg8000.exceptions"]

    def __init__(self, connection_params, client_encoding):
        params = self.get_connection_parameters(connection_params)
        self.conn = self.m.dbapi.connect(**params)
        self.conn.autocommit = False
        with contextlib.closing(self.conn.cursor()) as cur:
            cur.execute(f"SET client_encoding='{client_encoding}'")
        self.unsupported_type = self.m.exceptions.DatabaseError
        self.integrity_error = self.m.exceptions.DatabaseError

    def get_connection_parameters(self, connection_params):
        with db_connection.conninfo(connection_params) as conninfo:
            parameters = {
                "user": conninfo.user,
                "database": conninfo.dbname,
            }
            host = conninfo.host
            if host.startswith("/"):
                sock = f"{host}/.s.PGSQL.{conninfo.port}"
                if os.path.exists(sock):
                    parameters["unix_sock"] = sock
                    return parameters
                parameters["host"] = "localhost"
            else:
                parameters["host"] = host
            parameters["port"] = conninfo.port
            parameters["password"] = conninfo.password
            return parameters

    @staticmethod
    def supports_encoding(encoding):
        return encoding.upper() == "UTF8"

    @classmethod
    def get_pgcode(cls, err):
        return err.args[0]["C"]
