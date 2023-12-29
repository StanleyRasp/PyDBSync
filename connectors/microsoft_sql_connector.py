import time

import pyodbc
import datetime as dt

import support_functions as sf
from connectors.database_connector import DatabaseConnector, NoneType


class MicrosoftSQLConnector(DatabaseConnector):
    def __init__(self, user: str, password: str, host: str, port: str, database: str) -> None:
        login_details: dict = {
            "user": user,
            "password": password,
            "host": host,
            "port": port,
            "database": database
        }

        type_transforms = {
            str: "'{arg}'",
            dt.datetime: "'{arg}'",
            bool: lambda x: '1' if x else '0',
            NoneType: "NULL"
        }

        super().__init__(login_details, self.__ms_sql_connect, {}, type_transforms)

    @classmethod
    def from_file(cls, filename):
        """
        Creates a PostgresConnector instance from a JSON file.
        :param filename: JSON file in format:
        {
            "user": user,
            "password": password,
            "host": host,
            "port": port,
            "database": database
        }
        :return: a new PostgresConnector instance
        """
        login_details = sf.get_json_from_file(filename)
        return cls(
            login_details["user"],
            login_details["password"],
            login_details["host"],
            login_details["port"],
            login_details["database"]
        )

    def __ms_sql_connect(self):
        dsn: str = (f'DRIVER={{ODBC Driver 18 for SQL Server}};'
                    f'SERVER={self.login_details["host"]};DATABASE={self.login_details["database"]};'
                    f'Uid={self.login_details["user"]};PWD={self.login_details["password"]};'
                    f'Encrypt=yes;'
                    f'TrustServerCertificate=no;'
                    f'Connection Timeout=30;')

        return pyodbc.connect(dsn)
