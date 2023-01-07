import abc
import json
from typing import Any, Optional
from redis import Redis


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class RedisStorage(BaseStorage):
    def __init__(self, redis_adapter):
        self.redis_adapter = redis_adapter

    def save_state(self, new_state: dict) -> None:
        old_state = self.retrieve_state()

        old_state.update(new_state)

        old_state = json.dumps(old_state)
        self.redis_adapter.set('state', old_state)

    def retrieve_state(self):
        state_from_redis = self.redis_adapter.get('state')
        try:
            result = json.loads(state_from_redis)
        except Exception as ex:
            result = dict()
            # print(ex)
        return result


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def save_state(self, new_state):
        old_state = self.retrieve_state()

        old_state.update(new_state)

        with open(self.file_path, 'w') as f:
            json.dump(old_state, f)

    def retrieve_state(self):
        try:
            with open(self.file_path, 'r') as f:
                state_from_file = json.load(f)
        except FileNotFoundError:
            state_from_file = dict()
        return state_from_file


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, state_storage: BaseStorage):
        self.storage = state_storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        self.storage.save_state({key: value})

    def get_state(self, key=None) -> Any:
        """Получить состояние по определённому ключу"""
        return self.storage.retrieve_state().get(key, None)
