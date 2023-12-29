from data_sources.data_source import DataSource
from data_types.database_table import DatabaseTable
from unidecode import unidecode
import support_functions as sf

from state_controllers.state_controller import StateController


class DatabaseDataSource(DataSource):
    def __init__(self, table_name: str,
                 source_state_controller: StateController,
                 target_state_controller: StateController):
        """
        Creates a database data source object
        :param table_name: name of the resulting table 
        :param source_state_controller: source database state controller
        :param target_state_controller: target database state controller
        """
        self.table_name: str = table_name
        self.source_state_controller: StateController = source_state_controller
        self.target_state_controller: StateController = target_state_controller
        
        self.target_state: DatabaseTable = None
        self.source_state: DatabaseTable = None

        self.unidecode_on = True  # apply unidecode to fetched strings when comparing states

    # TODOXD: add primary key comparison optimisation
    def compare_states(self, target_state: DatabaseTable, source_state: DatabaseTable) -> int:
        """
        Returns -1 if the states are the same, otherwise returns the index of the first row with a change
        :param target_state:
        :param source_state:
        :return:
        """
        if target_state.column_count != source_state.column_count:
            raise Exception("The actual state has more columns that the saved state")
        # if target_state.header_row != where bil_datautworzenia > '2023-11-18 11:30:00'source_state.header_row:
        #     sf.print_to_console("WARNING: The column names are different!")

        for i in range(source_state.get_row_count()):
            if target_state.get_row_count() <= i: return i
            for j, elem in enumerate(source_state[i]):
                if type(elem) == str:
                    if self.unidecode_on and unidecode(elem) != unidecode(target_state[i][j]): return i
                    if not self.unidecode_on and elem != target_state[i][j]: return i
                elif elem != target_state[i][j]: return i

        return -1

    def has_change(self, *args: dict) -> bool:
        """
        :param args: optional kwargs for state queries given in order source_state_kwargs, target_state_kwargs
        target_kwargs are by default equal to source kwargs unless specified
        :return: there is (True) or there is no (False) change
        """

        source_kwargs, target_kwargs = self.__determine_kwargs(*args)

        self.target_state = self.target_state_controller.get_state(**target_kwargs)
        self.source_state = self.source_state_controller.get_state(**source_kwargs)

        return self.compare_states(self.target_state, self.source_state) != -1

    def get_change(self, *args: dict) -> DatabaseTable:
        source_kwargs, target_kwargs = self.__determine_kwargs(*args)
        
        self.source_state = self.source_state_controller.get_state(**source_kwargs)
        self.target_state = self.target_state_controller.get_state(**target_kwargs)

        change = DatabaseTable(self.table_name, self.target_state.get_header_row())

        diff: int = self.compare_states(self.target_state, self.source_state)

        while diff != -1:
            change.add_row(self.source_state[diff])
            self.source_state.discard_row(diff)
            diff: int = self.compare_states(self.target_state, self.source_state)

        return change

    def __determine_kwargs(self, *args) -> list[dict, dict]:
        if len(args) == 0:
            return [{}, {}]
        elif len(args) == 1:
            return [args[0], args[0]]
        else:
            return [args[0], args[1]]

    def set_unidecode(self, state: bool) -> None:
        """
        Set unidecode
        :param state: true if unidecode is true, else false
        :return: None
        """
        self.unidecode_on = state

