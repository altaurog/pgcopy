import contextlib
import sys

import psycopg2
import pytest
from psycopg2.extras import LoggingConnection

from . import db_connection
from .adaptor import available_adaptors
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
def client_encoding(request):
    return getattr(request, "param", "UTF8")


@pytest.fixture(params=available_adaptors())
def adaptor(request, db, client_encoding):
    if not request.param.supports_encoding(client_encoding):
        pytest.skip("Unsupported encoding for {request.param}")
    adaptor = request.param(connection_params, client_encoding)
    inst = request.instance
    if isinstance(inst, TemporaryTable):
        if db_connection.IS_DSQL:
            yield from dsql_table(adaptor, inst)
        else:
            yield from temporary_table(adaptor, inst)
    else:
        yield from no_table(adaptor)


def dsql_table(adaptor, inst):
    for adaptor in temporary_table(adaptor, inst):
        conn = adaptor.conn
        conn.commit()
        yield conn
        conn.commit()
        if drop_sql := inst.drop_sql():
            with contextlib.closing(conn.cursor()) as cur:
                cur.execute(drop_sql)
            conn.commit()


def temporary_table(adaptor, inst):
    for adaptor in no_table(adaptor):
        # use psycopg2 connection to create extensions if necessary
        psycopg2_conn = connect()
        for extension in inst.extensions:
            try:
                with psycopg2_conn.cursor() as cur:
                    cur.execute("CREATE EXTENSION {}".format(extension))
                psycopg2_conn.commit()
            except (
                psycopg2.errors.DuplicateObject,
                psycopg2.errors.UndefinedFile,  # postgres <= 14
                psycopg2.errors.FeatureNotSupported,  # postgres >= 15
            ):
                psycopg2_conn.rollback()
        psycopg2_conn.close()

        conn = adaptor.conn
        try:
            with contextlib.closing(conn.cursor()) as cur:
                cur.execute(inst.create_sql(inst.tempschema))
        except adaptor.unsupported_type as e:
            pgcode = adaptor.get_pgcode(e)
            conn.rollback()
            if pgcode == "42704":
                pytest.skip("Unsupported datatype")
            else:
                raise
        yield adaptor


def no_table(adaptor):
    conn = adaptor.conn
    yield adaptor
    conn.rollback()
    conn.close()


@pytest.fixture
def conn(adaptor):
    return adaptor.conn


@pytest.fixture
def integrity_error(adaptor):
    return adaptor.integrity_error


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
