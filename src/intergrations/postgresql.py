import json
import operator
import uuid
from abc import ABC
from dataclasses import dataclass
from typing import List, Dict
from datetime import datetime, date
import sqlalchemy
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import text
from sqlalchemy import inspect
from pypika import Query, Table, Field
from pypika import functions as fn

from src.data_types import ARRAY
from src.filter_manager import Filters
from src.intergrations.abstract_integrations import OutputIntegration, InputIntegration
from src.utils.common import create_session_new, commit_session
from src.utils.constants import SynchronizationState, ProdNumber, EngineType

Base = declarative_base()

arg_types_converting_methods = {
    str: lambda arg: arg if arg is None else (arg if isinstance(arg, str) else str(arg)),
    int: lambda arg: arg if arg is None else (arg if isinstance(arg, int) else int(arg)),
    float: lambda arg: arg if arg is None else (arg if isinstance(arg, float) else float(arg)),
    bool: lambda arg: arg if arg is None else (arg if isinstance(arg, bool) else bool(arg)),
    date: lambda arg: arg if arg is None else (arg if isinstance(arg, date) else datetime.strptime(arg, "%Y-%m-%d")),
    datetime: lambda arg: arg if arg is None else (
        arg if isinstance(arg, datetime) else datetime.strptime(arg, "%Y-%m-%d %H:%M:%S")),
    list: lambda arg, elem_type: arg if arg is None else [
        arg_types_converting_methods[elem_type.get_base_type()](item) for item in arg
    ],
    dict: lambda arg: arg if arg is None else (arg if isinstance(arg, dict) else json.loads(arg))
}


@dataclass
class Postgres(ABC):
    """
        Base class for interacting with a PostgreSQL database, utilizing an abstract base class (ABC).

        Attributes:
            host (str): The host address of the PostgreSQL server.
            port (str): The port number for the PostgreSQL server.
            user (str): The username for authenticating to the PostgreSQL server.
            password (str): The password for authenticating to the PostgreSQL server.
            dbname (str): The name of the database.
            table_name (str): The name of the table.
            schema (dict): The schema of the table.
    """
    host: str
    port: str
    user: str
    password: str
    dbname: str
    table_name: str
    schema: dict

    def __post_init__(self):
        """
            Initializes the PostgreSQL connection, session, and synchronization state.
        """
        config_dict = self.create_config_dict()
        session = create_session_new(config_dict)
        self.log_state = SynchronizationState(uuid.uuid1(),
                                              dwh_session=session,
                                              prod_number=ProdNumber.prod01.value,
                                              sync_name=__file__)

    def create_config_dict(self):
        """
            Creates a configuration dictionary for the PostgreSQL connection.

            Returns:
                dict: A dictionary containing the PostgreSQL configuration.
        """
        config_dict = {
            'host': self.host,
            'user': self.user,
            'password': self.password,
            'port': self.port,
            'dbname': self.dbname,
            'schema': 'public',
            'engine_type': EngineType.postgres_retry.value
        }
        return config_dict

    def _prefetch_rows(self):
        primary_key_columns = [column_name for column_name, column_spec in self.schema.items() if
                               column_spec.get('primary_key', False)]

        if not primary_key_columns:
            raise ValueError("No PK in table")

        table_ref = Table(name=self.table_name, schema=self.schema_name)
        get_prefetch_rows_query = Query.from_(table_ref).select(*primary_key_columns)

        result = [
            tuple(prefetch_result) for prefetch_result in
            self.log_state.dwh_session.execute(text(get_prefetch_rows_query.get_sql())).fetchall()
        ]
        return result

    @staticmethod
    def _typecasting(schema: Dict, data_list: List[Dict]) -> List[Dict]:
        """
        Typecasts the values in the data list according to the schema.

        Args:
            data_list (List[Dict]): The list of dictionaries containing data to be typecasted.

        Returns:
            List[Dict]: The list of dictionaries with typecasted values according to the schema.
        """
        converted_data = []

        for data_row in data_list:
            converted_row = {}
            for column, value in data_row.items():
                column_info = schema.get(column)
                if column_info is not None:
                    column_type = column_info.get('type')
                    if column_type is not None:
                        try:
                            if isinstance(column_type, ARRAY):
                                conversion_function = arg_types_converting_methods.get(list)
                                converted_value = conversion_function(value, column_type.element_type)
                            else:
                                conversion_function = arg_types_converting_methods.get(column_type.get_base_type())
                                if conversion_function is not None:
                                    converted_value = conversion_function(value)
                                else:
                                    print(f'The type conversion function for type {column_type.get_base_type()} '
                                          f'is missing from the dictionary of functions.')
                                    converted_value = str(value)

                            converted_row[column] = converted_value
                        except ValueError:
                            print(f"Error converting value '{value}' for column '{column}' to type '{column_type}'")
                            converted_row[column] = value
                    else:
                        print(f"Column '{column}' does not have a 'type' specified in the schema.")
                else:
                    print(f"Column '{column}' does not have a corresponding entry in the schema.")
                    converted_row[column] = value
            converted_data.append(converted_row)

        return converted_data


@dataclass
class PostgresqlInput(InputIntegration, Postgres):
    """
        Class for fetching data from PostgreSQL, inheriting from InputIntegration and Postgres.

        Attributes:
            filters (Filters): The Filters object for managing data filters.
    """
    filters: Filters = None

    def __post_init__(self):
        """
            Calls the initialization method of the Postgres base class.
        """
        self.schema_name, self.table_name = self.table_name.split('.')
        Postgres.__post_init__(self)

    def get_sort_column(self):
        sort_columns_dict = {}
        multiply_sorted_columns = []
        for column_name, column_spec in self.schema.items():
            if column_spec.get('filter_column', False):
                sort_columns_dict['first_priority'] = [column_name]
            if column_spec.get('primary_key', False):
                sort_columns_dict['second_priority'] = [column_name]
            if column_spec.get('order_by', False):
                sort_columns_dict['third_priority'] = [column_name]
            multiply_sorted_columns.append(column_name)
        sort_columns_dict['fourth_priority'] = multiply_sorted_columns
        return sort_columns_dict.get('first_priority', sort_columns_dict.get('second_priority',
                                                                             sort_columns_dict.get('third_priority',
                                                                                                   sort_columns_dict.get(
                                                                                                       'fourth_priority'))))

    def get_data_from_postgres(self, offset: int, limit: int, updated_at: datetime):
        """
            Fetches data from PostgreSQL considering offset, limit, and last updated time.

            Args:
                offset (int): The offset for pagination.
                limit (int): The limit on the number of results.
                updated_at (datetime): The last updated time for filtering data.

            Returns:
                List[Dict]: A list of dictionaries containing the fetched data.
        """
        filter_column = self.get_sort_column()
        select_column = [col_name for col_name in self.schema.keys()]
        get_data_query = Query.from_(Table(name=self.table_name, schema=self.schema_name))
        get_data_query = get_data_query \
            .select(*select_column) \
            .orderby(*[Field(column) for column in filter_column]) \
            .limit(limit) \
            .offset(offset * limit)
        if updated_at:
            get_data_query = get_data_query \
                .where(Field(filter_column[0]) > updated_at)
        get_data_query = self.get_filter_query(get_data_query)

        current_df = [dict_list._mapping for dict_list in
                      self.log_state.dwh_session.execute(text(get_data_query.get_sql())).fetchall()]
        return current_df

    def get_filter_query(self, filter_query):
        """
            Forms the PostgreSQL filter based on conditions defined in the filters object.

            Args:
                filter_query: The initial filter query.

            Returns:
                The updated filter query.
        """
        if self.filters:
            operator_mapping = {
                '>': operator.gt,
                '<': operator.lt,
                '>=': operator.ge,
                '<=': operator.le,
                '!=': operator.ne,
                '=': operator.eq,
                'like': lambda field, pattern: field.like(pattern),
                'ilike': lambda field, pattern: field.ilike(pattern),
                'not_like': lambda field, pattern: field.not_like(pattern),
                'not_ilike': lambda field, pattern: field.not_ilike(pattern),
            }
            filter_conditions = self.filters.get_filter_conditions()
            for column, value in filter_conditions.items():
                if isinstance(value, dict):
                    for op, val in value.items():
                        filter_query = filter_query \
                            .where(operator_mapping[op](Field(column), val))
                elif isinstance(value, list):
                    filter_query = filter_query \
                        .where(Field(column).isin(value))
                else:
                    filter_query = filter_query \
                        .where(Field(column).eq(value))
        return filter_query

    def get_sync_length(self, max_filter_value):
        """
            Retrieves the number of rows matching the filter for synchronization.

            Args:
                max_filter_value: The maximum value for filtering.

            Returns:
                int: The number of matching rows.
        """
        get_data_query = (Query
                          .from_(Table(name=self.table_name, schema=self.schema_name))
                          .select(fn.Count('*')))
        get_data_query = self.get_filter_query(get_data_query)
        if max_filter_value:
            for column_name, column_spec in self.schema.items():
                if column_spec.get('filter_column', False):
                    get_data_query = get_data_query \
                        .where(Field(column_name) > max_filter_value)
                    break
        count = self.log_state.dwh_session.execute(text(get_data_query.get_sql())).first()[0]
        return count

    def get_data(self, offset: int, limit: int, updated_at: datetime) -> List[dict]:
        """
            Fetches and typecasts data from PostgreSQL.

            Args:
                offset (int): The offset for pagination.
                limit (int): The limit on the number of results.
                updated_at (datetime): The last updated time for filtering data.

            Returns:
                List[dict]: A list of dictionaries with typecasted data.
        """
        print('Get data from postgresql')
        data_dict = self.get_data_from_postgres(offset, limit, updated_at)
        print('Typecasting data dict')
        converted_data = self._typecasting(self.schema, data_dict)
        return converted_data


@dataclass
class PostgresqlOutput(OutputIntegration, Postgres):
    """
        Class for writing data to PostgreSQL, inheriting from OutputIntegration and Postgres.
    """
    force_prefetching: bool = False

    def __post_init__(self):
        """
            Calls the initialization method of the Postgres base class,
            and creates the dynamic class for SQLAlchemy ORM.
        """
        self.schema_name, self.table_name = self.table_name.split('.')
        Postgres.__post_init__(self)
        self.create_dynamic_class()
        self._truncate_table()

    def _truncate_table(self):

        if self.overwrite:
            truncate_query = text(f"truncate table {self.schema_name}.{self.table_name};")
            self.log_state.dwh_session.execute(truncate_query)
            commit_session(self.log_state.dwh_session, self.log_state)
            print(f'The table: {self.table_name} has been deleted for overwriting.')

    def create_dynamic_class(self):
        """
            Dynamically creates a SQLAlchemy ORM class based on the table schema.
        """

        class DynamicClass(Base):
            __tablename__ = self.table_name
            __table_args__ = {'schema': self.schema_name}

            for column_name, column_spec in self.schema.items():
                column_type_spec = column_spec.get('type')
                column_type = column_type_spec.sqlalchemy_type
                if callable(column_type):
                    column_type = column_type()
                primary_key = column_spec.get('primary_key', False)
                unique = column_spec.get('unique', False)
                kwargs = {}
                if primary_key:
                    kwargs['primary_key'] = True
                if unique:
                    kwargs['unique'] = True
                locals()[column_name] = sqlalchemy.Column(column_type, **kwargs)

        self.log_state.table_name = DynamicClass

    def modify_df_for_query(self, data: List[dict]):
        """
            Modifies the data list for query insertion.

            Args:
                data (List[dict]): The list of dictionaries containing data to be inserted.

            Returns:
                List: A list of modified rows ready for insertion.
        """
        table = self.log_state.table_name
        df_final = []
        for row in data:
            modified_row = table(**row)
            df_final.append(modified_row)
        return df_final

    def set_data(self, data: List[dict]):
        """
            Writes data to PostgreSQL by merging rows into the table.

            Args:
                data (List[dict]): The list of dictionaries containing data to be written.
        """
        data = self._typecasting(self.schema, data)
        print("modify df for writing with query")
        df_final = self.modify_df_for_query(data)
        print("Merge rows in table")
        for row in df_final:
            self.log_state.dwh_session.merge(row)
        print(f"uploaded {len(df_final)} rows")
        commit_session(self.log_state.dwh_session, self.log_state)

    def filter_field_value(self):
        """
            Retrieves the maximum value of the filter field for the table.

            Returns:
                The maximum value of the filter field.
        """
        for column_name, column_spec in self.schema.items():
            if column_spec.get('filter_column', False):
                column_type = column_spec.get('type')
                query = text(f"select max({column_name}) from {self.schema_name}.{self.table_name}")
                result = self.log_state.dwh_session.execute(query).first()[0]
                conversion_function = arg_types_converting_methods.get(column_type.get_base_type())
                return conversion_function(result)
        return None

    def delete_prefetch_rows(self, mismatch_bunch):
        deleted_rows = 0

        for ids in mismatch_bunch:
            pk_columns = inspect(self.log_state.table_name).primary_key
            filter_kwargs = {pk.name: value for pk, value in zip(pk_columns, ids)}
            self.log_state.dwh_session.query(self.log_state.table_name).filter_by(**filter_kwargs).delete()
            deleted_rows += 1
        commit_session(self.log_state.dwh_session, self.log_state)
        return deleted_rows
