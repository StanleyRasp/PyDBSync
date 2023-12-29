from data_types.tabular import TableSizeMismatchException


class DatabaseTableSchema:
    def __init__(self, name: str, header_row: list[str] | tuple[str], types_row: list[type] | tuple[type]):
        if len(types_row) != len(header_row): raise TableSizeMismatchException(
            "Lengths of header and type row are different!")

        self.table_name = name
        self.header_row = header_row
        self.types_row = types_row
        self.column_count = len(header_row)