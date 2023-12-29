from unittest import TestCase
from data_types.tabular import Tabular, TableOverflowException, TableSizeMismatchException, TabularDataException

class TestTabular(TestCase):
    def test_add_row(self):
        tabular: Tabular = Tabular(3)

        tabular.add_row([1, 2, 3])
        self.assertEqual([1, 2, 3], tabular.table[0])

        tabular.add_row([1, 2])
        self.assertEqual([1, 2, None], tabular.table[1])

        self.assertEqual(2, tabular.row_count)

        with self.assertRaises(TableSizeMismatchException):
            tabular.add_row([1, 2, 3, 4])

        with self.assertRaises(TableSizeMismatchException):
            tabular.add_row([1, 2], pad=False)

    def test_append_table(self):
        tabular: Tabular = Tabular(3)

        new_table = [
            [1, 2, 3],
            [1, 2, 3],
            [1, 2, 3]
        ]

        tabular.append_table(new_table)

        self.assertEqual(3, tabular.row_count)

        self.assertEqual(new_table, tabular.table)

    def test_restrict_size(self):
        tabular: Tabular = Tabular(3)
        tabular.add_row([1, 2, 3])
        tabular.add_row([1, 2, 3])
        tabular.add_row([1, 2, 3])

        tabular.restrict_size(5)

        tabular.add_row([1, 2, 3])
        tabular.add_row([1, 2, 3])

        with self.assertRaises(TableOverflowException):
            tabular.add_row([1, 2, 3])

        tabular.table = []

        with self.assertRaises(TableOverflowException):
            tabular.append_table([
                [1, 2, 3],
                [1, 2, 3],
                [1, 2, 3],
                [1, 2, 3],
                [1, 2, 3],
                [1, 2, 3]
            ])

    def test_get_header_row(self):
        tabular: Tabular = Tabular(3)
        tabular.header_row = ["hi", "hello", "yo"]

        self.assertEqual(["hi", "hello", "yo"], tabular.get_header_row())

    def test_set_header_row(self):
        tabular: Tabular = Tabular(3)
        tabular.append_table([
            ["hi", "hello", "yo"],
            [1, 2, 3]
        ])

        tabular.set_header_row()
        self.assertEqual(["hi", "hello", "yo"], tabular.header_row)

        with self.assertRaises(TabularDataException):
            tabular.set_header_row()

        tabular.set_header_row(["elo", "pimp", "yo"])
        self.assertEqual(["elo", "pimp", "yo"], tabular.header_row)

        with self.assertRaises(TabularDataException):
            tabular.set_header_row(["elo", "pimp", "elo"])

    def test_get_table(self):
        tabular: Tabular = Tabular(3)

        new_table = [
            [1, 2, 3],
            [1, 2, 3],
            [1, 2, 3]
        ]

        tabular.append_table(new_table)

        self.assertEqual(new_table, tabular.get_table())

    def test_get_column_index(self):
        tabular: Tabular = Tabular(3)
        tabular.append_table([
            ["hi", "hello", "yo"],
            [1, 2, 3]
        ])

        tabular.set_header_row()

        self.assertEqual(1, tabular.get_column_index("hello"))

    def test_get_item(self):
        tabular: Tabular = Tabular(3)
        tabular.append_table([
            ["hi", "hello", "yo"],
            [1, 2, 3],
            ["uno", "dos", "tres"]
        ])

        tabular.set_header_row()

        self.assertEqual(["uno", "dos", "tres"], tabular[1])
        self.assertEqual(2, tabular[0, 1])
        self.assertEqual("dos", tabular[1, "hello"])
