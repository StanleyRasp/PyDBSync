import psycopg2 as pcpg
import support_functions as sf
from connectors.database_connector import DatabaseConnector, NoneType
import datetime as dt

class PostgresConnector(DatabaseConnector):
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
            bool: lambda x: str(x).lower(),
            NoneType: "NULL"
        }

        super().__init__(login_details, self.__postgres_connect, {}, type_transforms)

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

    def __postgres_connect(self):
        dsn: str = (f"dbname={self.login_details['database']} "
                    f"host={self.login_details['host']} "
                    f"port={self.login_details['port']} "
                    f"user={self.login_details['user']} "
                    f"password={self.login_details['password']} "
                    f"connect_timeout=3600")
        return pcpg.connect(dsn)

