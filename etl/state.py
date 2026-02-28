import abc
import json
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


class BaseStorage(abc.ABC):
    """Абстрактное хранилище состояния."""
    @abc.abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""
        pass


class JsonFileStorage(BaseStorage):
    """Реализация хранилища в JSON-файле."""
    def __init__(self, file_path: str):
        self.file_path = file_path

    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохраняет состояние в файл."""
        with open(self.file_path, 'w') as f:
            json.dump(state, f)

    def retrieve_state(self) -> Dict[str, Any]:
        """Извлекает состояние из файла."""
        try:
            with open(self.file_path, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}


class State:
    """Класс для работы с состояниями конкретных ключей."""
    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа."""
        state = self.storage.retrieve_state()
        state[key] = value
        self.storage.save_state(state)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу."""
        state = self.storage.retrieve_state()
        return state.get(key)
