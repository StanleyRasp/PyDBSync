from state_controllers.database_table_state_controller import DatabaseTableStateController
from data_types.database_table import DatabaseTable
from connectors.database_connector import DatabaseConnector


class VirtualViewDatabaseTableStateController(DatabaseTableStateController):
    def __init__(self, db_connector: DatabaseConnector, table_name: str, query: str, virtual_view_query: str):
        """
        Creates a DatabaseTableStateController
        :param db_connector: DatabaseConnector object on which a query will be performed
        :param table_name: name of the state table
        :param query: query string on which .format() will be called with the given kwargs when fetching the state
        indicate the virtual view table as v_view in your query
        :param virtual_view_query: a query on top of which the query will be called
        """

        self.virtual_view_query = virtual_view_query
        self.db_connector: DatabaseConnector = db_connector
        self.table_name: str = table_name
        self.query: str = query

        super().__init__(db_connector, table_name, query)

    def get_state(self, **kwargs) -> DatabaseTable:
        """
        Returns the state in a DatabaseTable object.
        :param kwargs: arguments to be placed into query.format(**kwargs) before running the query
        :return:
        """
        self.query = self.query.replace("v_view", f"({self.virtual_view_query}) as v_view")
        return self.get_state()
