from abc import abstractmethod


class StateController:
    @abstractmethod
    def get_state(self, **kwargs):
        pass
