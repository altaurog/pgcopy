import os
import sys
import psycopg2
from psycopg2.extras import LoggingConnection
import pytest
from .db import TemporaryTable

connection_params = {
    'dbname': os.getenv('POSTGRES_DB', 'pgcopy_test'),
    'port': int(os.getenv('POSTGRES_PORT', '5432')),
    'host': os.getenv('POSTGRES_HOST'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
}


@pytest.fixture(scope='session')
def db():
    drop, conn = create_db()
    yield conn
    if drop:
        drop_db(conn)


def connect(**kwargs):
    kw = connection_params.copy()
    kw.update(kwargs)
    conn = psycopg2.connect(connection_factory=LoggingConnection, **kw)
    conn.initialize(sys.stderr)
    return conn


def create_db():
    "connect to test db"
    try:
        return False, connect()
    except psycopg2.OperationalError as exc:
        nosuch_db = 'database "%s" does not exist' % connection_params['dbname']
        if nosuch_db in str(exc):
            try:
                master = connect(dbname='postgres')
                master.rollback()
                master.autocommit = True
                cursor = master.cursor()
                cursor.execute('CREATE DATABASE %s' % connection_params['dbname'])
                cursor.close()
                master.close()
            except psycopg2.Error as exc:
                message = ('Unable to connect to or create test db '
                            + connection_params['dbname']
                            + '.\nThe error is: %s' % exc)
                raise RuntimeError(message)
            else:
                return True, connect()


def drop_db(conn):
    "Drop test db"
    conn.close()
    master = connect(dbname='postgres')
    master.rollback()
    master.autocommit = True
    cursor = master.cursor()
    cursor.execute('DROP DATABASE %s' % connection_params['dbname'])
    cursor.close()
    master.close()


@pytest.fixture
def conn(request, db):
    db.autocommit = False
    cur = db.cursor()
    inst = request.instance
    if isinstance(inst, TemporaryTable):
        try:
            cur.execute(inst.create_sql(inst.tempschema))
        except psycopg2.ProgrammingError as e:
            db.rollback()
            if '42704' == e.pgcode:
                pytest.skip('Unsupported datatype')
    cur.close()
    yield db
    db.rollback()

@pytest.fixture
def cursor(conn):
    return conn.cursor()

@pytest.fixture
def schema(request, cursor):
    inst = request.instance
    if isinstance(inst, TemporaryTable):
        if not inst.tempschema:
            return 'public'
        cursor.execute("""
            SELECT nspname
            FROM   pg_namespace
            WHERE  oid = pg_my_temp_schema()
        """)
        return cursor.fetchall()[0][0]

@pytest.fixture
def schema_table(request, schema):
    inst = request.instance
    if isinstance(inst, TemporaryTable):
        return '{}.{}'.format(schema, inst.table)

@pytest.fixture
def data(request):
    inst = request.instance
    if isinstance(inst, TemporaryTable):
        return inst.data or inst.generate_data(inst.record_count)

