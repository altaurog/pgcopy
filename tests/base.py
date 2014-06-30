from . import get_conn

numname = lambda i: chr(ord('a') + i)

class DBTable(object):
    null = 'NOT NULL'
    def setUp(self):
        self.conn = get_conn()
        self.conn.autocommit = False
        self.cur = self.conn.cursor()
        self.table = self.__class__.__name__.lower()
        colsql = []
        for i, coltype in enumerate(self.datatypes):
            colsql.append('%s %s %s' % (numname(i), coltype, self.null))
        self.cur.execute(
                "CREATE TEMPORARY TABLE %s (" % self.table
                + ', '.join(colsql)
                + ");"
            )

    def tearDown(self):
        self.conn.rollback()
