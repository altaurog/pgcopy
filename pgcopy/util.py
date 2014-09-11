import re
import random
import string
from datetime import datetime
from pytz import UTC

def to_utc(dt):
    if not isinstance(dt, datetime):
        dt = datetime(dt.year, dt.month, dt.day)
    if dt.tzinfo is None:
        return UTC.localize(dt)
    else:
        return dt.astimezone(UTC)

source = string.ascii_lowercase + string.digits
def uid():
    vals = [random.choice(source) for i in range(5)]
    return ''.join(vals)


idre = lambda name: re.compile(r'\b%s\b' % re.escape(name))

class Replace(object):
    """
    Utility for fast updates on table involving most rows in the table.
    Instead of executemany("UPDATE ..."), create and populate
    a new table (which can be done using COPY), then rename.

    Can do this only if no other tables in the db depend on the table.

    NOTE: With PostgreSQL < 9.3 after the table is dropped attempts to
    query it will fail.

    See http://dba.stackexchange.com/a/41111/9941
    """
    def __init__(self, connection, table):
        self.cursor = connection.cursor()
        self.uid = uid()
        self.table = table
        self.name_re = idre(table)
        self.temp_name = self.newname()
        self.rename = [('TABLE', self.temp_name, table)]
        self.inspect()

    def __enter__(self):
        self.create_temp()
        self.create_defaults()
        return self.temp_name

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.create_notnull()
            self.create_constraints()
            self.create_indices()
            self.create_triggers()
            self.swap()
            self.create_views()
        self.cursor.close()

    def inspect(self):
        defquery = """
            SELECT attname, pg_get_expr(adbin, adrelid)
            FROM pg_attribute
            JOIN pg_attrdef ON attrelid = adrelid AND adnum = attnum
            WHERE adnum > 0
            AND attrelid = %s::regclass;
            """
        self.cursor.execute(defquery, (self.table,))
        self.defaults = self.cursor.fetchall()
        seqquery = """
            SELECT attname, relname FROM pg_class
            JOIN pg_depend ON (objid = pg_class.oid)
            JOIN pg_attribute ON (attnum=refobjsubid AND attrelid=refobjid)
            WHERE relkind = 'S'
            AND refobjid = %s::regclass
            """
        self.cursor.execute(seqquery, (self.table,))
        self.sequences = self.cursor.fetchall()
        attquery = """
            SELECT attname
            FROM pg_catalog.pg_attribute
            WHERE attrelid = %s::regclass
            AND attnum > 0 AND attnotnull
            """
        self.cursor.execute(attquery, (self.table,))
        self.notnull = [an for (an,) in self.cursor]
        # primary key is recreated as a constraint, 
        # but all other unique constraints are only
        # recreated as unique index
        conquery = """
            SELECT DISTINCT contype, conname, pg_catalog.pg_get_constraintdef(oid)
            FROM pg_catalog.pg_constraint
            WHERE conrelid = %s::regclass AND contype != 'u'
            """
        self.cursor.execute(conquery, (self.table,))
        self.constraints = self.cursor.fetchall()
        indquery = """
            SELECT c.relname, pg_catalog.pg_get_indexdef(i.indexrelid)
            FROM pg_catalog.pg_index i
            JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
            WHERE NOT indisprimary
            AND indrelid = %s::regclass
            """
        self.cursor.execute(indquery, (self.table,))
        self.indices = self.cursor.fetchall()
        trigquery = """
            SELECT tgname, pg_catalog.pg_get_triggerdef(oid)
            FROM pg_catalog.pg_trigger
            WHERE tgconstraint = 0
            AND tgrelid=%s::regclass
            """
        self.cursor.execute(trigquery, (self.table,))
        self.triggers = self.cursor.fetchall()
        viewquery = """
            SELECT DISTINCT c.relname, pg_get_viewdef(r.ev_class)
            FROM pg_rewrite r
            JOIN pg_depend d ON d.objid = r.oid
            JOIN pg_class c ON c.oid = r.ev_class
            WHERE d.refobjid = %s::regclass;
            """
        self.cursor.execute(viewquery, (self.table,))
        self.views = self.cursor.fetchall()

    def create_temp(self):
        create = 'CREATE TABLE "%s" AS TABLE "%s" WITH NO DATA'
        self.cursor.execute(create % (self.temp_name, self.table))

    def create_defaults(self):
        defsql = 'ALTER TABLE "%s" ALTER COLUMN "%s" SET DEFAULT %s'
        for col, default in self.defaults:
            self.cursor.execute(defsql % (self.temp_name, col, default))

    def create_notnull(self):
        nnsql = 'ALTER TABLE "%s" ALTER COLUMN "%s" SET NOT NULL'
        for col in self.notnull:
            self.cursor.execute(nnsql % (self.temp_name, col))

    def create_constraints(self):
        consql = 'ALTER TABLE "%s" ADD CONSTRAINT "%s" %s'
        for i, (contype, conname, condef) in enumerate(self.constraints):
            newname = self.newname('con', i)
            self.cursor.execute(consql % (self.temp_name, newname, condef))
            if 'p' == contype:
                self.rename.append(('INDEX', newname, conname))

    def create_indices(self):
        for i, (oldidxname, indexsql) in enumerate(self.indices):
            newidxname = self.newname('idx', i)
            newsql = self.sqlrename(indexsql, oldidxname, newidxname)
            self.cursor.execute(newsql)
            self.rename.append(('INDEX', newidxname, oldidxname))

    def create_triggers(self):
        for i, (oldtrigname, trigsql) in enumerate(self.triggers):
            newtrigname = self.newname('tg', i)
            newsql = self.sqlrename(trigsql, oldtrigname, newtrigname)
            self.cursor.execute(newsql)
            self.rename.append(('TRIGGER',
                                '%s" ON "%s' % (newtrigname, self.table),
                                oldtrigname))

    def swap(self):
        self.drop_views()
        self.drop_defaults()
        self.move_sequences()
        self.drop_original_table()
        self.rename_temp_table()

    def drop_views(self):
        for view, viewdef in self.views:
            self.cursor.execute('DROP VIEW "%s"' % view)

    def drop_defaults(self):
        dropdefsql = 'ALTER TABLE "%s" ALTER COLUMN "%s" DROP DEFAULT'
        for col, default in self.defaults:
            self.cursor.execute(dropdefsql % (self.table, col))

    def move_sequences(self):
        seqownersql = 'ALTER SEQUENCE "%s" OWNED BY "%s"."%s"'
        for col, seq in self.sequences:
            self.cursor.execute(seqownersql % (seq, self.temp_name, col))

    def drop_original_table(self):
        self.cursor.execute('DROP TABLE "%s"' % self.table)

    def rename_temp_table(self):
        sql = 'ALTER %s "%s" RENAME TO "%s"'
        for rename in self.rename:
            self.cursor.execute(sql % rename)

    def create_views(self):
        viewsql = 'CREATE VIEW "%s" AS %s'
        for view in self.views:
            sql = viewsql % view
            self.cursor.execute(sql)


    unsafe_re = re.compile(r'\W+')
    def newname(self, pre=None, i=None):
        parts = ['%s']
        vals = [self.table]
        if pre is not None:
            parts.append('%s')
            vals.append(pre)
        if i is not None:
            parts.append('%02d')
            vals.append(i)
        parts.append('%s')
        vals.append(self.uid)
        return self.unsafe_re.sub('', '_'.join(parts) % tuple(vals)).lower()

    def sqlrename(self, sql, *args):
        newsql = self.name_re.sub(self.temp_name, sql)
        try:
            old, new = args
        except ValueError:
            return newsql
        else:
            return idre(old).sub(new, newsql)


class RenameReplace(Replace):
    "Subclass for renaming old table and recreating empty one like it"
    def __init__(self, connection, table, xform):
        """
        xform must be a function which translates old
        names to new ones, used on tables & pk constraints
        """
        super(RenameReplace, self).__init__(connection, table)
        self.xform = xform

    def drop_original_table(self):
        pass

    def rename_temp_table(self):
        sql = 'ALTER %s "%s" RENAME TO "%s"'
        for objtype, temp, orig in self.rename:
            print(objtype, temp, orig)
            new_name = self.xform(orig)
            self.cursor.execute(sql % (objtype, orig, new_name))
        super(RenameReplace, self).rename_temp_table()


def rename_replace(connection, table, xform):
    with RenameReplace(connection, table, xform):
        pass
