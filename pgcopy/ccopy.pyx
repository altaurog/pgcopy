cimport cython
cimport numpy as np
from posix.unistd cimport write

cdef extern from "endian.h":
    ctypedef unsigned short uint16_t
    ctypedef unsigned int   uint32_t
    uint32_t htobe32(uint32_t)
    uint16_t htobe16(uint16_t)

cdef BINCOPY_HEADER = b'PGCOPY\n\377\r\n\0' + 8 * '\0'
cdef short BINCOPY_TRAILER = -1

cdef int NULL = -1
cdef unsigned int size[3]
for i, s in enumerate([1,4,8]):
    size[i] = htobe32(s)

def write_dataframe(fd, df):
    writedf(fd,
            df.iloc[:,0].astype('int32').values,
            df.iloc[:,1].astype('int64').values,
            df.iloc[:,2].values,
            df.iloc[:,3].values,
            df.iloc[:,4].values,)

@cython.boundscheck(False)
@cython.wraparound(False)
cdef writedf(
            int fd,
            np.ndarray[int] acol,
            np.ndarray bcol,
            np.ndarray[double] ccol,
            np.ndarray[object] dcol,
            np.ndarray ecol,
        ):
    cdef Py_ssize_t i, n = len(acol)
    cdef long a
    cdef long b
    cdef double c
    cdef char* d
    cdef char e
    cdef unsigned short count = 5
    count = htobe16(count)

    cdef unsigned int varsize = 0

    acol = acol.byteswap()
    bcol = ((bcol - 946684800000000000)/1000).byteswap()
    ccol = ccol.byteswap()

    write(fd, b'PGCOPY\n\377\r\n\0', 11)
    write(fd, &varsize, 4)
    write(fd, &varsize, 4)
    for i in range(n):
        write(fd, &count, 2)

        a = acol[i]
        write(fd, size + 1, 4)
        write(fd, &a, 4)

        b = bcol[i]
        write(fd, size + 2, 4)
        write(fd, &b, 8)

        c = ccol[i]
        write(fd, size + 2, 4)
        write(fd, &c, 8)

        d = dcol[i]
        varsize = htobe32(len(d))
        write(fd, &varsize, 4)
        write(fd, d, len(d))

        e = ecol[i]
        write(fd, size, 4)
        write(fd, &e, 1)
    write(fd, &BINCOPY_TRAILER, 2)
