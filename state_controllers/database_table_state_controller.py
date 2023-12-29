from data_types.database_table import DatabaseTable
from state_controllers.state_controller import StateController
from connectors.database_connector import DatabaseConnector


class DatabaseTableStateController(StateController):
    def __init__(self, db_connector: DatabaseConnector, table_name: str, query: str):
        """
        Creates a DatabaseTableStateController
        :param db_connector: DatabaseConnector object on which a query will be performed
        :param table_name: name of the state table
        :param query: query string on which .format() will be called with the given kwargs when fetching the state
        """

        self.db_connector: DatabaseConnector = db_connector
        self.table_name: str = table_name
        self.query: str = query

    def get_state(self, **kwargs) -> DatabaseTable:
        """
        Returns the state in a DatabaseTable object.
        :param kwargs: arguments to be placed into query.format(**kwargs) before running the query
        :return:
        """

        formatted_query = self.query.format(**kwargs)
        state = self.db_connector.execute_sql_query(formatted_query, self.table_name)
        return state
