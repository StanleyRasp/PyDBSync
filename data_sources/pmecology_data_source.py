import datetime

import support_functions as sf
from connectors.pmecology_rest_api_connector import PmecologyRestApiConnector
import datetime as dt

from data_types.tabular import Tabular
from data_types.database_table import DatabaseTable


class PmecologyDataSource:
    def __init__(self, key: str):
        self.api_connector: PmecologyRestApiConnector = PmecologyRestApiConnector(key)
        self.default_datetime: dt.datetime = dt.datetime(2023, 1, 1)
        self.last_timestamp = self.default_datetime

    @classmethod
    def from_file(cls, filepath: str):
        json_data = sf.get_json_from_file(filepath)
        return cls(json_data["key"])

    def has_change(self) -> bool:
        data: Tabular = self.api_connector.get_tabular_sensor_data(self.last_timestamp, 1)

        if data is None: return False
        return True

    def get_change(self) -> DatabaseTable:
        data: Tabular = self.api_connector.get_tabular_sensor_data(self.last_timestamp, 1000)

        database_table = DatabaseTable("test_wth",
                                       [
                                           "timestamp", "kierunek_wiatru", "predkosc_wiatru",
                                           "opad_atmosferyczny", "temperatura", "wilgotnosc", "cisnienie"],
                                       [dt.datetime, float, float, float, float, float, float])

        if data is None: return database_table

        database_table.append_table(data.get_table())

        

        return database_table
