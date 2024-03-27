import os
import re
import sys

import psycopg2
import pytest
from psycopg2.extras import LoggingConnection

from .db import TemporaryTable


def get_port():
    # this would be much more straightforward if tox-docker would release
    # recent updates https://github.com/tox-dev/tox-docker/pull/167
    if os.getenv("TOX_ENV_NAME"):
        search_pattern = re.compile(r"PG\w+_5432_TCP_PORT")
        for name, val in os.environ.items():
            if search_pattern.fullmatch(name):
                return int(val)
    return int(os.getenv("POSTGRES_PORT", "5432"))


connection_params = {
    "dbname": os.getenv("POSTGRES_DB", "pgcopy_test"),
    "port": get_port(),
    "host": os.getenv("POSTGRES_HOST"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}


@pytest.fixture(scope="session")
def db():
    drop = create_db()
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
    inst = request.instance
    if isinstance(inst, TemporaryTable):
        for extension in inst.extensions:
            try:
                with conn.cursor() as cur:
                    cur.execute("CREATE EXTENSION {}".format(extension))
                conn.commit()
            except (
                psycopg2.errors.DuplicateObject,
                psycopg2.errors.UndefinedFile,  # postgres <= 14
                psycopg2.errors.FeatureNotSupported,  # postgres >= 15
            ):
                conn.rollback()
        try:
            with conn.cursor() as cur:
                cur.execute(inst.create_sql(inst.tempschema))
        except psycopg2.errors.UndefinedObject as e:
            conn.rollback()
            pytest.skip("Unsupported datatype")
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
