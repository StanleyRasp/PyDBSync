from state_controllers.state_controller import StateController
from connectors.pmecology_rest_api_connector import PmecologyRestApiConnector


class PmecologyStateController(StateController):
    def __init__(self, api_connector: PmecologyRestApiConnector, table_name: str):
        """
        Creates the pmecology state controller
        :param api_connector: the appropriate PmecologyRestApiConnector
        :param table_name: name of the resulting table
        """

        self.api_connector: PmecologyRestApiConnector = api_connector
        self.table_name: str = table_name

    def get_state(self, **kwargs):
        """
        Gets the currents state of the source
        :param kwargs: utilised kwargs are:
        - timestamp - datetime from which to fetch data
        - records - the number of records to fetch
        - descending - boolean value for descending / ascending
        :return:
        """
        return self.api_connector.get_sensor_data(self.table_name,
                                                  kwargs["timestamp"],
                                                  kwargs["records"],
                                                  kwargs["descending"])

