import os
from io import BytesIO

from pgcopy import CopyManager

from . import db


class TestSanity(db.TemporaryTable):
    manager = CopyManager
    method = "copy"
    record_count = 3
    datatypes = [
        "integer",
        "timestamp with time zone",
        "double precision",
        "varchar(12)",
        "bool",
    ]

    def test_sanity(self, conn, schema_table, data):
        mgr = self.manager(conn, schema_table, self.cols)
        datastream = BytesIO()
        mgr.writestream(data, datastream)
        datastream.seek(0)
        assert self.expected_output == datastream.read()

    expected_output = (
        b"PGCOPY\n\xff\r\n\x00\x00\x00\x00\x00"
        b"\x00\x00\x00\x00\x00\x05\x00\x00\x00\x04\x00\x00\x00\x00\x00"
        b"\x00\x00\x08\xff\xfc\xa2\xfe\xc4\xc8 \x00\x00\x00\x00\x08"
        b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0ccfc"
        b"d208495d5\x00\x00\x00\x01\x01\x00"
        b"\x05\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00\x08\xff\xfc"
        b"\xa2\xff\x9b[\xc4\x00\x00\x00\x00\x08?\xf2\x00\x00\x00"
        b"\x00\x00\x00\x00\x00\x00\x0bc4ca4238"
        b"a0b\x00\x00\x00\x01\x00\x00\x05\x00\x00\x00\x04\x00"
        b"\x00\x00\x02\x00\x00\x00\x08\xff\xfc\xa3\x00q\xefh\x00"
        b"\x00\x00\x00\x08@\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        b"\nc81e728d9d\x00\x00\x00\x01"
        b"\x00\xff\xff"
    )
