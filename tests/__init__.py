import psycopg2

db_state = {
        'test_db': 'pgcopy_test',
        'conn': None,
        'drop': False,
    }

def set_conn(conn):
    db_state['conn'] = conn

def get_conn():
    return db_state.get('conn')

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
    master = psycopg2.connect(database='postgres')
    master.autocommit = True
    cursor = master.cursor()
    cursor.execute('DROP DATABASE %s' % db_state['test_db'])
    cursor.close()
    master.close()

def setup():
    conn = create_db()
    set_conn(conn)

def teardown():
    if db_state['drop']:
        drop_db()
