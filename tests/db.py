from datetime import datetime, timedelta
import hashlib
import math
import psycopg2

db_state = {
        'test_db': 'pgcopy_test',
        'conn': None,
        'drop': False,
    }

def get_conn():
    conn = db_state.get('conn')
    if conn is None:
        conn = create_db()
        db_state['conn'] = conn
    return conn

def create_db():
    "connect to test db"
    try:
        conn = psycopg2.connect(database=db_state['test_db'])
    except psycopg2.OperationalError, exc:
        nosuch_db = 'database "%s" does not exist' % db_state['test_db']
        if nosuch_db in str(exc):
            try:
                master = psycopg2.connect(database='postgres')
                master.autocommit = True
                cursor = master.cursor()
                cursor.execute('CREATE DATABASE %s' % db_state['test_db'])
                cursor.close()
                master.close()
            except psycopg2.Error, exc:
                message = ('Unable to connect to or create test db '
                            + db_state['test_db']
                            + '.\nThe error is: %s' % exc)
                raise RuntimeError(message)
            else:
                conn = psycopg2.connect(database=db_state['test_db'])
                db_state['drop'] = True
    return conn

def drop_db():
    "Drop test db"
    if not db_state['drop']:
        return
    master = psycopg2.connect(database='postgres')
    master.autocommit = True
    cursor = master.cursor()
    cursor.execute('DROP DATABASE %s' % db_state['test_db'])
    cursor.close()
    master.close()


datagen = {
        'bool': lambda i: 0 == (i % 3),
        'integer': lambda i: i,
        'double precision': lambda i: math.pi * i,
        'timestamp with time zone':
            lambda i: datetime(1970, 1, 1) + timedelta(hours=i),
        'varchar(12)':
            lambda i: hashlib.md5(str(i)).hexdigest()[:12],
    }

colname = lambda i: chr(ord('a') + i)

class TemporaryTable(object):
    null = 'NOT NULL'
    def setUp(self):
        self.conn = get_conn()
        self.conn.autocommit = False
        self.cur = self.conn.cursor()
        self.table = self.__class__.__name__.lower()
        colsql = []
        for i, coltype in enumerate(self.datatypes):
            colsql.append('%s %s %s' % (colname(i), coltype, self.null))
        self.cur.execute(
                "CREATE TEMPORARY TABLE %s (" % self.table
                + ', '.join(colsql)
                + ");"
            )

    def generate_data(self, count):
        data = []
        gen = [datagen[t] for t in self.datatypes]
        for i in xrange(count):
            row = [g(i) for g in gen]
            data.append(tuple(row))
        return data

    def tearDown(self):
        self.conn.rollback()
