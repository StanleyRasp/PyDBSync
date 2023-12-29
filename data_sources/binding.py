from abc import abstractmethod


class Binding:
    @abstractmethod
    def check(self) -> None:
        pass