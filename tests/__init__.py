from . import db


def setup():
    conn = db.get_conn()
    assert conn is not None, 'Connection to db failed with params: {}'.format(db.db_state)


def teardown():
    conn = db.get_conn()
    assert conn is not None, 'Connection to db failed with params: {}'.format(db.db_state)
    db.drop_db()
