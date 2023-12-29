from data_types.tabular import *

class DatabaseTable(Tabular):
    def __init__(self, name: str, header_row: list[str] | tuple[str]):
        super().__init__(len(header_row))
        self.name = name

        self.set_header_row(header_row)

    def get_table_name(self) -> str:
        return self.name