from cStringIO import StringIO
from pgcopy import CopyManager
from . import db


class Sanity(db.TemporaryTable):
    manager = CopyManager
    method = 'copy'
    record_count = 3
    datatypes = [
            'integer',
            'timestamp with time zone',
            'double precision',
            'varchar(12)',
            'bool',
        ]

    def sanity(self):
        mgr = self.manager(self.conn, self.table, self.cols)
        datastream = StringIO()
        mgr.writestream(self.data, datastream)
        datastream.seek(0)
        self.assertCorrect(datastream.read())

    def assertCorrect(self, test):
        assert self.expected_output == test

    expected_output = (
            'PGCOPY\n\xff\r\n\x00\x00\x00\x00\x00'
            '\x00\x00\x00\x00\x00\x05\x00\x00\x00\x04\x00\x00\x00\x00\x00'
            '\x00\x00\x08\xff\xfc\xa2\xfe\xc4\xc8 \x00\x00\x00\x00\x08'
            '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0ccfc'
            'd208495d5\x00\x00\x00\x01\x01\x00'
            '\x05\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00\x08\xff\xfc'
            '\xa2\xff\x9b[\xc4\x00\x00\x00\x00\x08?\xf2\x00\x00\x00'
            '\x00\x00\x00\x00\x00\x00\x0bc4ca4238'
            'a0b\x00\x00\x00\x01\x00\x00\x05\x00\x00\x00\x04\x00'
            '\x00\x00\x02\x00\x00\x00\x08\xff\xfc\xa3\x00q\xefh\x00'
            '\x00\x00\x00\x08@\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00'
            '\nc81e728d9d\x00\x00\x00\x01'
            '\x00\xff\xff'
        )
