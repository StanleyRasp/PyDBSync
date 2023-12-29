class Tabular:
    def __init__(self, columns: int):
        self.header_row: list[str] = []
        self.table: list[list] = []

        self.row_count: int = 0
        self.column_count: int = columns

        self.row_restrict: int = -1

    def restrict_size(self, rows: int = -1) -> None:
        """
        Restricts the maximum size of the table.

        :param rows: max size allowed for table row_count
        """

        if rows < self.row_count:
            raise Exception(f"row_count already bigger than {rows}")

        self.row_restrict = self.row_restrict if rows == -1 else rows

    def add_row(self, row: tuple|list, pad: bool = True) -> None:
        """
        Validates and adds a row to the table
        :param row: row data
        :param pad: whether to pad the remaining horizontal space with None
        :return: None
        :raises: TableOverflowException
        :raises: TableSizeMismatchException
        """
        if len(row) > self.column_count:
            raise TableSizeMismatchException("Table overflow, row has to many columns")

        if self.row_restrict != -1 and self.row_count >= self.row_restrict:
            raise TableOverflowException("Table overflow, maximum number of rows reached")

        if not pad and len(row) != self.column_count:
            raise TableSizeMismatchException("Appended table's column count is different and padding is off.")

        new_row: list = []

        for obj in row:
            new_row.append(obj)

        while pad and len(new_row) < self.column_count:
            new_row.append(None)

        self.table.append(new_row)

        self.row_count += 1

    def append_table(self, table: tuple[tuple] | list[tuple] | list[list], pad: bool = True):
        """
        Appends a passed table to self
        :param table: table data
        :param pad: whether to pad missing column data with None
        :return: None
        :raises: TableOverflowException
        :raises: TableSizeMismatchException
        """
        for i, row in enumerate(table):
            try:
                self.add_row(row, pad)
            except TableSizeMismatchException as e:
                raise TableSizeMismatchException(e.message + f"\nAppended table's row {i+1} does not match.")

    def set_header_row(self, header_row: tuple[str] | list[str] = None) -> None:
        """
        Sets a header row either by taking the top row from the table, or to the value of the passed row
        :param header_row: header row data (has to be a list or tuple of string)
        :return: None
        :raises TableSizeMismatchException
        :raises TabularDataException
        """
        def __validate_header_row(row: tuple[str] | list[str]):
            if len(row) != self.column_count:
                raise TableSizeMismatchException(f"Header row has a different ({len(row)}) amount of columns")
            if len(row) > len(set(row)):
                raise TabularDataException("Header row values are not unique")

        if header_row is not None:
            __validate_header_row(header_row)
            self.header_row = header_row
            return None


        if self.table[0] is None: raise TableSizeMismatchException("There is no header row to read!")

        new_header_row = []
        for i, obj in enumerate(self.table[0]):
            value = self.table[0][i]
            if type(value) is not str:
                raise TabularDataException(f"Header row's data is not string. Index {i}")
            new_header_row.append(value)

        __validate_header_row(new_header_row)
        self.header_row = new_header_row

        self.table = self.table[1:]
        self.row_count -= 1

    def get_row_count(self):
        return self.row_count

    def get_header_row(self) -> list[str]:
        return self.header_row

    def get_table(self) -> list:
        return self.table

    def get_column_index(self, column: str) -> int:
        if len(self.header_row) <= 0:
            raise TabularDataException("Table has no header row to reference!")
        try:
            return self.header_row.index(column)
        except ValueError as e:
            raise TabularDataException(f"No column of name {column}")

    def discard_top_row(self) -> list:
        discarded_row = self.table[0]
        self.table = self.table[1:]
        self.row_count -= 1
        return discarded_row

    def discard_row(self, index: int) -> list:
        discarded_row = self.table[index]
        self.table = self.table[:index] + self.table[index+1:]
        self.row_count -= 1
        return discarded_row

    def clear_table(self):
        self.row_count = 0
        self.table.clear()

    def is_empty(self) -> bool:
        return self.row_count == 0 or (self.row_count == 1 and (self[0] is None or self[0][0] is None))

    def __add__(self, other: tuple[tuple] | list[tuple] | list[list]):
        self.append_table(other)

    def __getitem__(self, item: int | tuple[int, str] | tuple[int, int]):
        if type(item) is int:
            return self.table[item]

        if type(item) is tuple:
            if len(item) != 2: raise TabularDataException("The table is 2 dimensional!")
            row = self.table[item[0]]
            if type(item[1]) is int:
                return row[item[1]]
            if type(item[1]) is str:
                return row[self.get_column_index(item[1])]

        raise TabularDataException("Incorrect format!")

    def __len__(self) -> int:
        return len(self.table)


class TabularDataException(Exception):
    def __init__(self, error_message: str):
        self.message = error_message
        super().__init__(error_message)

class TableOverflowException(TabularDataException):
    pass

class TableSizeMismatchException(TabularDataException):
    pass