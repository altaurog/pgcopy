import argparse
import importlib
import sys


def available_adaptors():
    adaptors = [Psycopg2, Psycopg3]
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


class Psycopg3(Adaptor):
    module_names = ["psycopg"]

    def __init__(self, connection_params, client_encoding):
        self.conn = self.m.psycopg.connect(**connection_params)
        self.conn.autocommit = False
        self.conn.execute(f"SET client_encoding='{client_encoding}'")
        self.unsupported_type = self.m.psycopg.errors.UndefinedObject
