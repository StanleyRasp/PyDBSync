from abc import abstractmethod


class DataSource:
    @abstractmethod
    def has_change(self, *args) -> bool:
        pass

    @abstractmethod
    def get_change(self, *args) -> object:
        pass