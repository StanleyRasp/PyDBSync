from time import sleep
from typing import Callable

import types
import support_functions as sf
from data_types.database_table import DatabaseTable

class NoneType:
    pass

class DatabaseConnector:
    def __init__(self, login_details: dict, connect: Callable,
                 type_mapping: dict = {}, type_transforms: dict = {}) -> None:
        """
        :param login_details: dictionary of login details in format:
        {
            "user": user,
            "password": password,
            "host": host,
            "port": port,
            "database": database
        }
        :param connect: a function returning the connection object adhering to python's DBAPI
        :param type_mapping: a dictionary mapping types to their respective names in a specific DBMS
        :param type_transforms: a dictionary mapping data types to strings in format "foo{arg}bar"
        on which .format(arg: element) will be called for every element of that type before inserting.
        Alternatively a Callable object can be used as func(element) -> str
        If a type is not present str(element) will be called
        Use the database_connector.NoneType class to decide what to do with None types
        """

        self.type_transforms = type_transforms
        self.type_mapping = type_mapping
        self.connect_func: Callable = connect
        self.login_details: dict = login_details

        # connection object adhering to python's DBAPI
        self.connection = None

        # time in seconds to wait in between reconnect attempts
        self.reconnect_wait_time = 10

    def connect(self, reconnect_wait_time: int):
        """Assures that a connection is established after this method returns,
        if connection attempt is unsuccessful retry after reconnect_wait_time seconds"""
        sf.print_to_console(f"Connecting to "
                            f"{self.get_login_details()['user']}"
                            f"@{self.get_login_details()['host']} ...")
        self.connection = self.connect_func()
        while not self.__is_connection_open():
            sleep(reconnect_wait_time)
            sf.print_to_console(f"Reconnecting to "
                                f"{self.get_login_details()['user']}"
                                f"@{self.get_login_details()['host']} ...")
            self.connection = self.connect_func()

    def insert_data(self, data: DatabaseTable):
        columns: str = ", ".join(data.get_header_row())
        rows: str = ""

        for row in data.get_table():
            row = self.__transform_row_for_insertion(row)
            rows += "(" + ", ".join(row) + "),\n"

        rows = rows[:-2]
        sql_command = f"""
                INSERT INTO {data.get_table_name()} 
                ({columns})
                VALUES 
                {rows}
                """

        self.execute_sql_statement(sql_command)

    def execute_sql_query(self, query: str, table_name: str) -> DatabaseTable:
        """
        :param query: sql query
        :param table_name: name of the resulting table
        :return: DatabaseTable object constructed from the result of the query
        """
        if not self.__is_connection_open():
            self.connect(self.reconnect_wait_time)

        with self.connection.cursor() as cur:
            cur.execute(query)
            column_names = [desc[0] for desc in cur.description]
            db_table =  DatabaseTable(table_name, column_names)
            db_table.append_table(cur.fetchall())
            return db_table

    def execute_sql_statement(self, statement: str) -> None:
        if not self.__is_connection_open():
            self.connect(self.reconnect_wait_time)

        with self.connection.cursor() as cur:
            cur.execute(statement)
            self.connection.commit()

    def disconnect(self):
        if self.connection is None: raise NoConnectionEstablishedException(
            "Connect was not called! No available connection")

        self.connection.close()

    def get_login_details(self) -> dict:
        return self.login_details

    def __is_connection_open(self) -> bool:
        if self.connection is None: return False
        return self.connection.closed == 0

    def __transform_row_for_insertion(self, row: list[any] | tuple[any]) -> list[any]:
        result = []
        for elem in row:
            elem_type = NoneType if elem is None else type(elem)
            if elem_type in self.type_transforms.keys():
                if type(self.type_transforms[elem_type]) == str:
                    result.append(self.type_transforms[elem_type].format(arg=elem))
                elif type(self.type_transforms[elem_type]) == types.FunctionType:
                    result.append(self.type_transforms[elem_type](elem))
                else:
                    raise IllegalTransformTypeException(
                    f"A transform type of {type(self.type_transforms[elem_type])} not allowed in type_transforms!")
            else:
                result.append(str(elem))

        return result

class DatabaseConnectorException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(message)

class NoConnectionEstablishedException(DatabaseConnectorException):
    def __init__(self, message):
        super().__init__(message)

class IllegalTransformTypeException(DatabaseConnectorException):
    def __init__(self, message):
        super().__init__(message)