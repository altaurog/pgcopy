import contextlib

import pytest

from . import db_connection
from .adaptor import available_adaptors
from .db import TemporaryTable

# pylint: disable=redefined-outer-name,consider-using-f-string


@pytest.fixture(scope="session")
def db():
    drop = db_connection.create_db()
    yield
    if drop:
        db_connection.drop_db()


@pytest.fixture
def client_encoding(request):
    return getattr(request, "param", "UTF8")


@pytest.fixture(params=available_adaptors())
def adaptor(request, db, client_encoding):
    if not request.param.supports_encoding(client_encoding):
        pytest.skip("Unsupported encoding for {request.param}")
    adaptor = request.param(db_connection.connection_params, client_encoding)
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
        yield adaptor
        conn.commit()
        if drop_sql := inst.drop_sql():
            with contextlib.closing(conn.cursor()) as cur:
                cur.execute(drop_sql)
            conn.commit()


def temporary_table(adaptor, inst):
    for adaptor in no_table(adaptor):
        if inst.extensions:
            db_connection.create_extensions(inst.extensions)

        conn = adaptor.conn
        try:
            with contextlib.closing(conn.cursor()) as cur:
                cur.execute(inst.create_sql(inst.tempschema))
        except adaptor.unsupported_type as e:
            pgcode = adaptor.get_pgcode(e)
            conn.rollback()
            if pgcode in ("42704", "0A000"):
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
