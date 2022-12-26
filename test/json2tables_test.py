import unittest


class Json2TablesTestCase(unittest.TestCase):
    def test_tables_concat(self):

        tables = ['0', '1', '2', '3', '4', '5']
        new_table = None
        iter_range = range(len(tables) - 1)
        for i in iter_range:
            if i == 0:
                new_table = tables[0]
            if i < len(tables):
                next_table = tables[i + 1]
                tables_argument = [new_table, next_table]
                result_table = tables_argument
                new_table = result_table
        self.assertEqual(True, new_table == [[[[['0', '1'], '2'], '3'], '4'], '5'])  # add assertion here


if __name__ == '__main__':
    unittest.main()
