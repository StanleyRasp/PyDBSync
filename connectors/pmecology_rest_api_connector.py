import support_functions as sf
import requests as rq
import requests.exceptions as rqe
import json
import datetime as dt
import time

from data_types.tabular import Tabular
from data_types.database_table import DatabaseTable


class PmecologyRestApiConnector:
    def __init__(self, key: str):
        self.api_key: str = key
        self.request_string: str = f"https://api.system.pmecology.com/v1/data/{self.api_key}?"
        self.channels: dict = json.loads(rq.get(f"{self.request_string}?").text)["channels"]

    @classmethod
    def from_file(cls, filename: str):
        return cls(sf.get_json_from_file(filename)["key"])

    def get_persistently(self, request_string: str, retry_wait_time: int = 5) -> rq.Response:
        req = None
        while req is None or req.status_code != 200:
            if req is not None:
                time.sleep(retry_wait_time)
                sf.print_to_console(f"Retrying request {request_string} ...")

            try:
                req = rq.get(request_string)
            except rqe.ConnectionError as e:
                sf.print_to_console(f"ConnectionError occurred:\n{e}Retrying request {request_string} ...")
                req = None
                continue

        return req

    def get_first_recorded_timestamp(self) -> dt.datetime:
        return dt.datetime.fromisoformat(json.loads(self.get_persistently(self.request_string).text)["first_record_timestamp"])

    def get_last_recorded_timestamp(self) -> dt.datetime:
        return dt.datetime.fromisoformat(json.loads(self.get_persistently(self.request_string).text)["last_record_timestamp"])

    def get_channels_data(self) -> dict:
        return json.loads(self.get_persistently(self.request_string).text)["channels"]

    def get_channel_name_mapping(self) -> dict[str: str]:
        mapping = {}
        for key in self.channels.keys():
            mapping[key] = self.channels[key]["name"]

        return mapping

    def __get_raw_sensor_data(self,
                              timestamp: dt.datetime = None,
                              records: int = None,
                              descending: bool = None) -> list[dict]:

        timestamp_string = "" if timestamp is None else "timestamp=" + sf.datetime_to_iso(timestamp)
        records_string = "" if records is None else "records=" + str(records)
        descending = "" if descending is None else "descending=" + str(descending).lower()

        parameters_string = "&".join([timestamp_string, records_string, descending])

        data_points = json.loads(self.get_persistently(self.request_string + parameters_string).text)["history"]

        return data_points

    def get_sensor_data(self, table_name: str,
                            timestamp: dt.datetime = None,
                            records: int = None,
                            descending: bool = None) -> DatabaseTable:
        """
        Fetches sensor data and returns them in a DatabaseTable object
        :param table_name: name of the returned DatabaseTable
        :param timestamp: fetch all data points greater than (>) timestamp
        :param records: the number of records to fetch
        :param descending: in descending (True) or increasing (False) order
        :return: a DatabaseTable instance containing returned data, with the header_row consisting of channel names,
        and a specified name
        """
        timestamp += dt.timedelta(seconds=1)

        raw_data = self.__get_raw_sensor_data(timestamp, records, descending)

        if len(raw_data) <= 0: return None

        name_mapping: dict = self.get_channel_name_mapping()

        column_names = ["timestamp"] + [value for value in name_mapping.values()]

        table = DatabaseTable(table_name, column_names)

        for data_point in raw_data:
            data_point_timestamp = dt.datetime.fromisoformat(data_point["timestamp"])
            if data_point_timestamp.minute % 5 != 0:
                continue
            row = len(column_names) * [None]
            row[0] = data_point_timestamp

            sensor_values: dict = data_point["values"]

            for key, value in sensor_values.items():
                row[column_names.index(name_mapping[key])] = value

            table.add_row(row, False)

        return table



