import os
import sys

import psycopg2
import pytest
from psycopg2.extras import LoggingConnection

from .db import TemporaryTable

connection_params = {
    "dbname": os.getenv("POSTGRES_DB", "pgcopy_test"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "host": os.getenv("POSTGRES_HOST"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}


@pytest.fixture(scope="session")
def db():
    drop = create_db()
    load_extensions()
    yield
    if drop:
        try:
            drop_db()
        except psycopg2.OperationalError:
            pass


def connect(**kwargs):
    kw = connection_params.copy()
    kw.update(kwargs)
    conn = psycopg2.connect(connection_factory=LoggingConnection, **kw)
    conn.initialize(sys.stderr)
    return conn


def create_db():
    "connect to test db"
    try:
        connect().close()
        return False
    except psycopg2.OperationalError as exc:
        nosuch_db = 'database "%s" does not exist' % connection_params["dbname"]
        if nosuch_db in str(exc):
            try:
                master = connect(dbname="postgres")
                master.rollback()
                master.autocommit = True
                cursor = master.cursor()
                cursor.execute("CREATE DATABASE %s" % connection_params["dbname"])
                cursor.close()
                master.close()
            except psycopg2.Error as exc:
                message = (
                    "Unable to connect to or create test db "
                    + connection_params["dbname"]
                    + ".\nThe error is: %s" % exc
                )
                raise RuntimeError(message)
            else:
                return True


def load_extensions():
    conn = connect()
    cur = conn.cursor()
    try:
        cur.execute("CREATE EXTENSION vector")
    except:
        pass


def drop_db():
    "Drop test db"
    master = connect(dbname="postgres")
    master.rollback()
    master.autocommit = True
    cursor = master.cursor()
    cursor.execute("DROP DATABASE %s" % connection_params["dbname"])
    cursor.close()
    master.close()


@pytest.fixture
def conn(request, db):
    conn = connect()
    conn.autocommit = False
    conn.set_client_encoding(getattr(request, "param", "UTF8"))
    cur = conn.cursor()
    inst = request.instance
    if isinstance(inst, TemporaryTable):
        try:
            cur.execute(inst.create_sql(inst.tempschema))
        except psycopg2.ProgrammingError as e:
            conn.rollback()
            if "42704" == e.pgcode:
                pytest.skip("Unsupported datatype")
    cur.close()
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture
def cursor(conn):
    cur = conn.cursor()
    yield cur
    cur.close()


@pytest.fixture
def schema(request, cursor):
    inst = request.instance
    if isinstance(inst, TemporaryTable):
        if not inst.tempschema:
            return "public"
        cursor.execute(
            """
            SELECT nspname
            FROM   pg_catalog.pg_namespace
            WHERE  oid = pg_catalog.pg_my_temp_schema()
        """
        )
        return cursor.fetchall()[0][0]


@pytest.fixture
def schema_table(request, schema):
    inst = request.instance
    if isinstance(inst, TemporaryTable):
        return "{}.{}".format(schema, inst.table)


@pytest.fixture
def data(request):
    inst = request.instance
    if isinstance(inst, TemporaryTable):
        return inst.data or inst.generate_data(inst.record_count)
