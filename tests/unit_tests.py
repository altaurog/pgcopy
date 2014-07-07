from unittest import TestCase
import numpy as np
import pandas as pd
try:
    import pyximport
    pyximport.install()
    from pgcopy import ccopy

    class CompileMixin(object):
        def setUp(self):
            type_dict = self.type_dict
            cols = list(self.type_dict.iterkeys())
            class Manager(ccopy.CopyManager):
                def get_types(self):
                    return type_dict
            self.mgr = Manager(None, 'test', cols)
            self.dtype = self.mgr.data_dtype
            data = [[1, 'hello'],[None, 'goodbye farewell']]
            self.data = pd.DataFrame(data, columns=['a', 'b'])

    class TestCompileInt(CompileMixin, TestCase):
        type_dict = {'a': ['int8', -1, False],
                    'b': ['varchar', 10, True],}

        def test_dtype(self):
            self.assertEqual(self.dtype.names, ('f0', 'f1', 'nf0'))
            fields = self.dtype.fields
            self.assertEqual(fields['f0'], (np.dtype('>i8'), 0))
            self.assertEqual(fields['f1'], (np.dtype('|S10'), 8))
            self.assertEqual(fields['nf0'], (np.dtype('i1'), 18))

        def test_prepare_data(self):
            a = self.mgr.prepare_data(self.data)
            self.assertEqual(a[0][0], 1)
            self.assertEqual(a[0][1], 'hello')
            self.assertEqual(a[0][2], 0)
            self.assertEqual(a[1][1], 'goodbye fa')
            self.assertEqual(a[1][2], 1)

        def test_writedata(self):
            expected = ('PGCOPY\n\xff\r\n\x00'
                        '\x00\x00\x00\x00\x00\x00\x00\x00'
                        '\x00\x02'
                        '\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x01'
                        '\x00\x00\x00\x05hello'
                        '\x00\x02'
                        '\xff\xff\xff\xff'
                        '\x00\x00\x00\x0agoodbye fa'
                        '\xff\xff')
            with open('testtemp.dat', 'w+b') as f:
                self.mgr.writestream(self.data, f.fileno())
                f.seek(0)
                self.assertEqual(f.read(), expected)
except ImportError:
    pass
