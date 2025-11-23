import sys

import psycopg2
import pytest
from psycopg2.extras import LoggingConnection

from . import db_connection
from .db import TemporaryTable

connection_params = db_connection.get_connection_params()


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
    inst = request.instance
    if isinstance(inst, TemporaryTable):
        if db_connection.IS_DSQL:
            yield from dsql_table(request, inst)
        else:
            yield from temporary_table(request, inst)
    else:
        yield from no_table(request)


def dsql_table(request, inst):
    for conn in temporary_table(request, inst):
        conn.commit()
        yield conn
        conn.commit()
        if drop_sql := inst.drop_sql():
            with conn.cursor() as cur:
                cur.execute(drop_sql)
            conn.commit()


def temporary_table(request, inst):
    for conn in no_table(request):
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
        except (
            psycopg2.errors.FeatureNotSupported,
            psycopg2.errors.UndefinedObject,
        ) as e:
            conn.rollback()
            pytest.skip("Unsupported datatype")
        yield conn


def no_table(request):
    conn = connect()
    conn.autocommit = False
    conn.set_client_encoding(getattr(request, "param", "UTF8"))
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
        return inst.generate_data()
