import datetime
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Dict

from src.data_types import check_registry


@dataclass
class Schemable(ABC):
    """
        Abstract base class for schema validation and storage of schema structure information.

        Attributes:
            schema (Dict): A dictionary representing the schema.
    """
    schema: Dict

    def __post_init__(self):
        self._validate_schema_types()

    def _validate_schema_types(self):
        """
            Validates the types in the schema against a registry of allowed types.

            Raises:
                TypeError: If an unregistered data type is found in the schema.
        """
        for name, data_type in self.schema.items():
            if not check_registry(data_type): raise TypeError(
                f"Unregistered data type {data_type} in your schema")


@dataclass
class InputIntegration(Schemable):
    """
        Abstract class for input integrations, extending the Schemable class for schema validation and storing schema structure information.
    """

    @abstractmethod
    def get_data(self, offset: int, limit: int, updated_at: datetime) -> List[dict]:
        """
            Abstract method for retrieving data from the input source.

            Args:
                offset (int): The offset for pagination.
                limit (int): The limit on the number of results.
                updated_at (datetime): The last updated time for filtering data.

            Returns:
                List[dict]: A list of dictionaries containing the retrieved data.
        """
        pass


@dataclass
class OutputIntegration(Schemable):
    """
        Abstract class for output integrations, extending the Schemable class for schema validation and storing schema structure information.
    """
    overwrite: bool = False

    @abstractmethod
    def set_data(self, data: List[dict]):
        """
            Abstract method for setting data to the output destination.

            Args:
                data (List[dict]): The list of dictionaries containing data to be written.
        """
        pass


@dataclass
class TableIntegrationClass(ABC):
    table_name: str
    table_name_source: str
    source_db_name: str
    force_prefetching: bool
    overwrite: bool

    @staticmethod
    @abstractmethod
    def schema_input():
        pass

    @staticmethod
    @abstractmethod
    def schema_output():
        pass
