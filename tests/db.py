import os
import sys
from datetime import datetime, date, timedelta
import hashlib
import psycopg2
from psycopg2.extras import LoggingConnection

from pgcopy import util

import pytest

db_state = {
        'connection_params': {
            'dbname': os.getenv('POSTGRES_DB', 'pgcopy_test'),
            'port': int(os.getenv('POSTGRES_PORT', '5432')),
            'host': os.getenv('POSTGRES_HOST'),
            'user': os.getenv('POSTGRES_USER'),
            'password': os.getenv('POSTGRES_PASSWORD'),
        },
        'conn': None,
        'drop': False,
    }

def connect(**kwargs):
    kw = db_state['connection_params'].copy()
    kw.update(kwargs)
    conn = psycopg2.connect(connection_factory=LoggingConnection, **kw)
    conn.initialize(sys.stderr)
    return conn


def get_conn():
    conn = db_state.get('conn')
    if conn is None:
        conn = create_db()
        db_state['conn'] = conn
    return conn

def create_db():
    "connect to test db"
    try:
        return connect()
    except psycopg2.OperationalError as exc:
        nosuch_db = 'database "%s" does not exist' % db_state['connection_params']['dbname']
        if nosuch_db in str(exc):
            try:
                master = connect(dbname='postgres')
                master.rollback()
                master.autocommit = True
                cursor = master.cursor()
                cursor.execute('CREATE DATABASE %s' % db_state['connection_params']['dbname'])
                cursor.close()
                master.close()
            except psycopg2.Error as exc:
                message = ('Unable to connect to or create test db '
                            + db_state['connection_params']['dbname']
                            + '.\nThe error is: %s' % exc)
                raise RuntimeError(message)
            else:
                conn = connect()
                db_state['drop'] = True
                return conn

def drop_db():
    "Drop test db"
    if not db_state['drop']:
        return
    get_conn().close()
    master = connect(dbname='postgres')
    master.rollback()
    master.autocommit = True
    cursor = master.cursor()
    cursor.execute('DROP DATABASE %s' % db_state['connection_params']['dbname'])
    cursor.close()
    master.close()

genbool = lambda i: 0 == (i % 3)
genint = lambda i: i
genfloat = lambda i: 1.125 * i
gendate = lambda i: date(1708, 1, 1) + timedelta(i % (250 * 365))
gendatetime = lambda i: datetime(1970, 1, 1) + timedelta(hours=i)
gendatetimetz = lambda i: util.to_utc(datetime(1970, 1, 1) + timedelta(hours=i))
genstr12 = lambda i: hashlib.md5(str(i).encode()).hexdigest()[:12 - (i % 3)].encode()

datagen = {
        'bool': genbool,
        'smallint': genint,
        'integer': genint,
        'bigint': genint,
        'real': genfloat,
        'double precision': genfloat,
        'date': gendate,
        'timestamp': gendatetime,
        'timestamp with time zone': gendatetimetz,
        'varchar(12)': genstr12,
        'char(12)': genstr12,
    }

colname = lambda i: chr(ord('a') + i)

class TemporaryTable(object):
    tempschema = True
    null = 'NOT NULL'
    data = None
    record_count = 0

    def temp_schema_name(self):
        cursor = self.conn.cursor()
        cursor.execute("""SELECT nspname
                          FROM   pg_namespace
                          WHERE  oid = pg_my_temp_schema()""")
        return cursor.fetchall()[0][0]

    def setup(self):
        self.conn = get_conn()
        self.conn.rollback()
        self.conn.autocommit = False
        self.cur = self.conn.cursor()
        self.table = self.__class__.__name__.lower()
        self.cols = [colname(i) for i in range(len(self.datatypes))]
        try:
            self.cur.execute(self.create_sql(self.tempschema))
        except psycopg2.ProgrammingError as e:
            self.conn.rollback()
            if '42704' == e.pgcode:
                pytest.skip('Unsupported datatype')

        if self.tempschema:
            self.schema = self.temp_schema_name()
        else:
            self.schema = 'public'
        self.schema_table = '{}.{}'.format(self.schema, self.table)

        if self.data is None and self.record_count > 0:
            self.data = self.generate_data(self.record_count)

    def create_sql(self, tempschema=None):
        colsql = [(c, t, self.null) for c, t in zip(self.cols, self.datatypes)]
        collist = ', '.join(map(' '.join, colsql))
        if tempschema:
            return "CREATE TEMPORARY TABLE {} ({})".format(self.table, collist)
        return "CREATE TABLE public.{} ({})".format(self.table, collist)

    def generate_data(self, count):
        gen = [datagen[t] for t in self.datatypes]
        return [tuple(g(i) for g in gen) for i in range(count)]

    def teardown(self):
        self.conn.rollback()
