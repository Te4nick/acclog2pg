from abc import ABC, abstractmethod
from typing import Any


class DB(ABC):
    @abstractmethod
    def create_job(self, job_id: str, values: dict[str: Any]):
        pass

    @abstractmethod
    def update_job(self, job_id: str, values: dict[str: Any]):
        pass

    @abstractmethod
    def has_job(self, job_id: str) -> bool:
        pass
