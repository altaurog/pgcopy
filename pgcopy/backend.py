"psycopg backends"
import importlib


class Psycopg2Backend:
    def __init__(self, conn):
        self.conn = conn
        self.adaptor = importlib.import_module("psycopg2")
        self.adaptor.extras = importlib.import_module("psycopg2.extras")

    def get_encoding(self):
        encodings = self.adaptor.extensions.encodings
        return encodings[self.conn.encoding]

    def copystream(self, sql, datastream):
        cursor = self.conn.cursor()
        cursor.copy_expert(sql, datastream)

    def namedtuple_cursor(self):
        factory = self.adaptor.extras.NamedTupleCursor
        return self.conn.cursor(cursor_factory=factory)
