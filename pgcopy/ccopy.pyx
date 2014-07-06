cimport cython
cimport numpy as np
from posix.unistd cimport write
from libc.string cimport strlen
from libc.stdlib cimport malloc, free

from datetime import date, datetime
import tempfile
import sys
import pandas as pd
import pytz
import time
from . import inspect

cdef extern from "string.h" nogil:
    size_t strnlen(char*, size_t maxlen)

cdef extern from "endian.h" nogil:
    ctypedef unsigned short uint16_t
    ctypedef unsigned int   uint32_t
    uint32_t htobe32(uint32_t)
    uint16_t htobe16(uint16_t)

cdef extern from "sys/uio.h" nogil:
    struct iovec:
        void *iov_base
        size_t iov_len
    ssize_t writev(int fd, iovec *iov, int iovcnt)

BINCOPY_HEADER = b'PGCOPY\n\377\r\n\0' + 8 * '\0'
BINCOPY_TRAILER = b'\xff\xff'
cdef int PG_NULL = -1  # Here be and le are the same

cdef struct Field:
    unsigned int offset
    unsigned int size
    unsigned int isnull_offset
    bint isnullable
    bint isstr

dtype_map = {
    'bool': 'i1',
    'int2': 'i2',
    'int4': 'i4',
    'int8': 'i8',
    'float4' : 'f4',
    'float8': 'f8',
    'varchar': 'a',
    'bpchar': 'a',      # (blank-padded) char
    'date': 'i',
    'timestamp': 'q',
    'timestamptz': 'q',
}

cdef class CopyManager:
    cdef Field* field
    cdef public object conn, table, cols, times
    cdef public object column_defs, data_dtype
    def __cinit__(self, conn, table, cols):
        self.field = <Field *>malloc(len(cols) * sizeof(Field))
        self.conn = conn
        self.table = table
        self.cols = cols
        self.times = {}
        self.column_defs = None

    def __init__(self, conn, table, cols):
        self.compile()

    def get_types(self):
        return inspect.get_types(self.conn, self.table)

    def compile(self):
        if self.column_defs is not None:
            return
        type_dict = self.get_types()
        self.column_defs = []
        dtype_def = []
        nulls = []
        for i, colname in enumerate(self.cols):
            try:
                type_info = type_dict[colname]
            except KeyError:
                message = '"%s" is not a column of table "%s"'
                raise ValueError(message % (colname, self.table))
            coltype, typemod, notnull = type_info
            try:
                dtype_str = dtype_map[coltype]
            except KeyError:
                message = '"%s" is not a supported datatype'
                raise ValueError(message % (coltype,))
            if typemod > -1:
                field_dtype = np.dtype('%s%d' % (dtype_str, typemod))
            else:
                field_dtype = np.dtype('>' + dtype_str)
            fname = 'f%d' % i
            dtype_def.append((fname, field_dtype))
            if not notnull:
                nulls.append(('n' + fname, 'b'))
            self.column_defs.append((colname, fname, coltype, typemod, notnull))
        dtype_def.extend(nulls)
        self.data_dtype = np.dtype(dtype_def, align=True)
        for i, coldef in enumerate(self.column_defs):
            colname, fname, coltype, typemod, notnull = coldef
            fdef = self.data_dtype.fields[fname]
            self.field[i].offset = fdef[1]
            self.field[i].size = fdef[0].itemsize
            self.field[i].isnullable = not notnull
            self.field[i].isstr = (typemod > -1)
            if not notnull:
                null_fdef = self.data_dtype.fields['n' + fname]
                self.field[i].isnull_offset = null_fdef[1]

    def copy(self, data):
        datastream = tempfile.TemporaryFile()
        datastream.write(BINCOPY_HEADER)
        datastream.flush()
        self.writestream(data, datastream)
        datastream.write(BINCOPY_TRAILER)
        datastream.flush()
        datastream.seek(0)
        self.copystream(datastream)
        datastream.close()

    def writestream(self, data, datastream):
        start = time.time()
        a = self.prepare_data(data)
        self.write_data(datastream, a)
        self.times['writestream'] = time.time() - start

    def prepare_data(self, df):
        self.compile()
        df_spec = {}
        for i, coldef in enumerate(self.column_defs):
            colname, fname, coltype, typemod, notnull = coldef
            if coltype == 'date':
                nanoseconds = (df[colname] - date(2000, 1, 1)).astype('i8')
                df_spec[fname] = nanoseconds/1e9/60/60/24
            elif coltype.startswith('timestamp'):
                epoch = datetime(2000, 1, 1)
                if df[colname].dtype is np.dtype('O'):
                    epoch = pytz.UTC.localize(epoch)
                df_spec[fname] = (df[colname] - epoch)/1000
            else:
                df_spec[fname] = df[colname]
            if not notnull:
                df_spec['n' + fname] = df[colname].isnull()
        return pd.DataFrame(df_spec).to_records(False).astype(self.data_dtype)

    def write_data(self, datastream, a):
        self._write_data(datastream.fileno(), a)

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef _write_data(self, int fd, np.ndarray records):
        cdef Py_ssize_t i, row_count = len(records)
        cdef Py_ssize_t j, field_count = len(self.cols)
        cdef uint16_t itemsize = records.itemsize
        cdef Field* field = self.field
        cdef char* data = <char *>records.data
        cdef char* fptr
        cdef uint16_t becount = field_count
        becount = htobe16(becount)
        cdef uint32_t fsize
        cdef uint32_t *befsize = <uint32_t *>malloc(field_count * sizeof(uint32_t))
        cdef iovec *wrv = <iovec *>malloc((2 * field_count + 1) * sizeof(iovec))
        cdef uint16_t k
        with nogil:
            wrv[0].iov_base = &becount
            wrv[0].iov_len = 2
            for i in range(row_count):
                k = 1
                for j in range(field_count):
                    if field[j].isnullable:
                        if data[field[j].isnull_offset]:
                            wrv[k].iov_base = &PG_NULL
                            wrv[k].iov_len = 4
                            k += 1
                            continue
                    fptr = data + field[j].offset
                    fsize = field[j].size
                    if field[j].isstr:
                        fsize = strnlen(fptr, fsize)
                    befsize[j] = htobe32(fsize)
                    wrv[k].iov_base = befsize + j
                    wrv[k].iov_len = 4
                    k += 1
                    wrv[k].iov_base = fptr
                    wrv[k].iov_len = fsize
                    k += 1
                writev(fd, wrv, k)
                data += itemsize
        free(befsize)
        free(wrv)

    def __dealloc__(self):
        free(self.field)

    def copystream(self, datastream):
        start = time.time()
        columns = '", "'.join(self.cols)
        sql = """COPY "{0}" ("{1}")
                FROM STDIN WITH BINARY""".format(self.table, columns)
        cursor = self.conn.cursor()
        try:
            cursor.copy_expert(sql, datastream)
        except Exception, e:
            message = "error doing binary copy into %s:\n%s" % (self.table, e.message)
            raise type(e), type(e)(message), sys.exc_info()[2]
        self.times['copystream'] = time.time() - start

