from . import db

def setup():
    db.get_conn()

def teardown():
    db.drop_db()

