import inspect
import uuid
from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta
from typing import Type
import sqlalchemy
from pyspark.sql.types import FloatType, IntegerType, StringType, DateType, TimestampType, BooleanType, ArrayType, \
    MapType
from sqlalchemy.dialects.postgresql import UUID

datatype_registry: list = []


class DataType(ABC):
    """
        Abstract base class for data types, defining the interface for base and PySpark types.
    """

    @property
    @abstractmethod
    def base_type(self): pass

    def pyspark_type(self): pass

    def sqlalchemy_type(self): pass

    @classmethod
    def get_base_type(cls):
        """
            Returns the base type of the data type.

            Returns:
                Type: The base type.
        """
        return cls.base_type

    @classmethod
    def get_pyspark_type(cls):
        """
            Returns the PySpark type of the data type.

            Returns:
                Type: The PySpark type.
        """
        return cls.pyspark_type

    @classmethod
    def get_sqlalchemy_type(cls):
        """
            Returns the base type of the data type.

            Returns:
                Type: The base type.
        """
        return cls.sqlalchemy_type


class UUIDType(DataType):
    base_type: Type = uuid.UUID
    pyspark_type: Type = StringType()
    sqlalchemy_type: Type = sqlalchemy.dialects.postgresql.UUID

    def __repr__(self):
        return "UUID"


class Integer(DataType):
    base_type: Type = int
    pyspark_type: Type = IntegerType()
    sqlalchemy_type: Type = sqlalchemy.Integer

    def __repr__(self):
        return "Integer"


class Float(DataType):
    base_type: Type = float
    pyspark_type: Type = FloatType()
    sqlalchemy_type: Type = sqlalchemy.Float

    def __repr__(self):
        return "Float"


class String(DataType):
    base_type: Type = str
    pyspark_type: Type = StringType()
    sqlalchemy_type: Type = sqlalchemy.String

    def __repr__(self):
        return "String"


class Date(DataType):
    base_type: Type = date
    pyspark_type: Type = DateType()
    sqlalchemy_type: Type = sqlalchemy.Date

    def __repr__(self):
        return "Date"


class DateTime(DataType):
    base_type: Type = datetime
    pyspark_type: Type = TimestampType()

    def __init__(self, with_timezone: bool = False):
        self.with_timezone = with_timezone

    @property
    def sqlalchemy_type(self):
        if self.with_timezone:
            return sqlalchemy.DATETIME(timezone=True)
        return sqlalchemy.DATETIME(timezone=False)

    def __repr__(self):
        return "DateTime"


class TimeDelta(DataType):
    base_type: Type = timedelta
    pyspark_type: Type = IntegerType()
    sqlalchemy_type: Type = sqlalchemy.Interval

    def __repr__(self):
        return "TimeDelta"


class BOOLEAN(DataType):
    base_type: Type = bool
    pyspark_type: Type = BooleanType()
    sqlalchemy_type: Type = sqlalchemy.BOOLEAN

    def __repr__(self):
        return "BOOLEAN"


class JSONB(DataType):
    base_type: Type = dict
    pyspark_type: Type = MapType(StringType(), StringType())
    sqlalchemy_type: Type = sqlalchemy.dialects.postgresql.JSONB

    def __repr__(self):
        return "JSONB"


class ARRAY(DataType):

    def __init__(self, element_type):
        if isinstance(element_type, type):
            self.element_type = element_type()
        else:
            self.element_type = element_type

    @property
    def base_type(self):
        return list

    @property
    def pyspark_type(self):
        return ArrayType(self.element_type.pyspark_type)

    @property
    def sqlalchemy_type(self):
        return sqlalchemy.ARRAY(self.element_type.sqlalchemy_type)

    def __repr__(self):
        return f"ARRAY({repr(self.element_type)})"


def register_type(data_type: Type[DataType]):
    """
        Registers a data type in the datatype registry.

        Args:
            data_type (Type[DataType]): The data type to be registered.

        Raises:
            TypeError: If the data type does not inherit from DataType.
    """
    if issubclass(data_type, DataType):
        datatype_registry.append(data_type)
    else:
        raise TypeError(f"Wrong type of inheritance {data_type}. Registered type must inherit from the DataType")


def check_registry(data_type: Type[DataType]) -> bool:
    """
        Checks if a data type is registered in the datatype registry.

        Args:
            data_type (Type[DataType]): The data type to be checked.

        Returns:
            bool: True if the data type is registered, False otherwise.
    """
    return data_type in datatype_registry


def register_all_datatypes_in_current_module():
    """
        Registers all data types defined in the current module in the datatype registry.
    """
    current_module = inspect.getmodule(inspect.currentframe())
    for name, obj in inspect.getmembers(current_module, inspect.isclass):
        if issubclass(obj, DataType) and obj is not DataType:
            register_type(obj)


register_all_datatypes_in_current_module()
