import json
import ssl
from abc import ABC
from dataclasses import dataclass
from datetime import datetime, date
from typing import List, Dict

from opensearchpy import OpenSearch, helpers
from elasticquery import ElasticQuery, Query as ESQuery
from opensearchpy.connection import create_ssl_context
from dateutil.parser import parse as date_parse
from src.filter_manager import Filters
from src.intergrations.abstract_integrations import InputIntegration, OutputIntegration

arg_types_converting_methods = {
    str: lambda arg: arg if arg is None else (arg if isinstance(arg, str) else str(arg)),
    int: lambda arg: arg if arg is None else (arg if isinstance(arg, int) else int(arg)),
    float: lambda arg: arg if arg is None else (arg if isinstance(arg, float) else float(arg)),
    bool: lambda arg: arg if arg is None else (arg if isinstance(arg, bool) else bool(arg)),
    date: lambda arg: arg if arg is None else (arg if isinstance(arg, date) else datetime.strptime(arg, "%Y-%m-%d")),
    datetime: lambda arg: arg if arg is None else (
        arg if isinstance(arg, datetime) else date_parse(arg)),
    list: lambda arg, elem_type: arg if arg is None else [
        arg_types_converting_methods[elem_type.get_base_type()](item) for item in arg
    ],
    dict: lambda arg: arg if arg is None else (arg if isinstance(arg, dict) else json.loads(arg))
}


@dataclass
class OpenSearchABC(ABC):
    """
        Abstract base class for interacting with OpenSearch.

        Attributes:
            hosts (str): The URI for connecting to OpenSearch.
            user (str): The username for authentication.
            secret (str): The password for authentication.
            port (int): The port number for connecting to OpenSearch.
            table_name (str): The name of the OpenSearch index (table).
    """
    hosts: str
    user: str
    secret: str
    port: int
    table_name: str

    def __post_init__(self):
        ssl_context = create_ssl_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        self.session = OpenSearch(
            hosts=self.hosts,
            http_auth=(self.user, self.secret),
            port=self.port,
            ssl_context=ssl_context
        )

    @staticmethod
    def _typecasting(schema: Dict, data_list: List[Dict]) -> List[Dict]:
        """
            Typecasts the values in the data list according to the schema.

            Args:
                schema (Dict): The schema defining the expected types for the data.
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
                            conversion_function = arg_types_converting_methods.get(column_type.get_base_type())
                            if conversion_function is not None:
                                converted_value = conversion_function(value)
                                converted_row[column] = converted_value
                            else:
                                print(f'The type conversion function for type {column_type.get_base_type()} '
                                      f'is missing from the dictionary of functions.')
                                converted_row[column] = str(value)
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
class OpenSearchInput(InputIntegration, OpenSearchABC):
    """
        Class for handling data input operations from OpenSearch.

        Attributes:
            filters (Filters): The filter object for managing query filters.
    """
    filters: Filters = None

    def __post_init__(self):
        OpenSearchABC.__post_init__(self)
        self.sort_columns = self._get_sort_column()

    def _get_sort_column(self):
        """
            Retrieves columns to be used for sorting data.

            Returns:
                list: A list of column names sorted by priority.
        """
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

    def get_filter_query(self, must_block: list):
        """
            Forms the OpenSearch filter based on conditions defined in the filters object.

            Args:
                must_block (list): The initial filter query.

            Returns:
                list: The updated filter query.
        """
        if self.filters:
            operator_mapping = {
                '>': lambda field, val_: ESQuery.range(field, gt=val_),
                '<': lambda field, val_: ESQuery.range(field, lt=val_),
                '>=': lambda field, val_: ESQuery.range(field, gte=val_),
                '<=': lambda field, val_: ESQuery.range(field, lte=val_),
                '!=': lambda field, val_: ESQuery.bool(must_not=[ESQuery.term(field, val_)]),
                '=': lambda field, val_: ESQuery.term(field, val_),
                'like': lambda field, val_: ESQuery.wildcard(field, f"*{val_}*"),
                'ilike': lambda field, val_: ESQuery.wildcard(field, f"*{val_}*", case_insensitive=True),
                'not_like': lambda field, val_: ESQuery.bool(
                    must_not=[ESQuery.wildcard(field, f"*{val_}*")]
                ),
                'not_ilike': lambda field, val_: ESQuery.bool(
                    must_not=[ESQuery.wildcard(field, f"*{val_}*", case_insensitive=True)]
                )
            }

            filter_conditions = self.filters.get_filter_conditions()
            for column, value in filter_conditions.items():
                if isinstance(value, dict):
                    for op, val in value.items():
                        condition = operator_mapping[op](column, val)
                        must_block.append(condition)
                elif isinstance(value, list):
                    must_block.append(ESQuery.terms(column, value))
                else:
                    must_block.append(ESQuery.terms(column, [value]))
        return must_block

    def _build_query(self, updated_at: datetime):
        """
            Builds the OpenSearch query based on the provided update timestamp.

            Args:
                updated_at (datetime): The timestamp to filter updated records.

            Returns:
                ElasticQuery: The constructed query object.
        """
        must_block = list()
        query = ElasticQuery()
        if updated_at:
            must_block.append(ESQuery.range(self.sort_columns[0], gt=updated_at))
        must_block = self.get_filter_query(must_block)
        bool_block = ESQuery.bool(must=must_block)
        query.query(bool_block)

        return query

    def flatten(self, dictionary: dict, separator: str = '.', prefix: str = '') -> dict:
        """
        Recursively flattens input dictionary with nested dictionaries inside of values
        :param dictionary: input dictionary for flattening
        :param separator: separator for keys concatenating
        :param prefix: prefix for keys concatenating
        :return: flat dictionary without nested dictionary inside
        """
        return {prefix + separator + k if prefix else k: v
                for kk, vv in dictionary.items()
                for k, v in self.flatten(vv, separator, kk).items()
                } if isinstance(dictionary, dict) else {prefix: dictionary}

    def _read_rows_from_opensearch(self, query, limit: int, offset: int):
        """
            Reads rows from OpenSearch based on the provided query, limit, and offset.

            Args:
                query (ElasticQuery): The query object for OpenSearch.
                limit (int): The number of records to fetch.
                offset (int): The offset for fetching records.

            Returns:
                list: List of dictionaries containing the fetched data.
        """
        for col_name in self.sort_columns:
            query = query.sort(col_name, order='asc')
        response = self.session.search(_source=[col_name for col_name in self.schema.keys()], index=self.table_name,
                                       from_=offset * limit, size=limit, body=query.json(), timeout=30)
        return [hit['_source'] for hit in response['hits']['hits']]

    def get_data(self, offset: int, limit: int, updated_at: datetime) -> List[dict]:
        """
            Fetches data from OpenSearch, typecasts it, and returns it.

            Args:
                offset (int): The offset for fetching records.
                limit (int): The number of records to fetch.
                updated_at (datetime): The timestamp to filter updated records.

            Returns:
                List[dict]: The fetched and typecasted data.
        """
        query = self._build_query(updated_at)
        data = self._read_rows_from_opensearch(query, limit, offset)
        converted_data = self._typecasting(self.schema, data)

        return converted_data

    def get_sync_length(self, max_filter_value):
        """
            Retrieves the number of rows matching the filter for synchronization.

            Args:
                max_filter_value: The maximum value for filtering.

            Returns:
                int: The number of matching rows.
        """
        query = self._build_query(max_filter_value)
        count = self.session.count(body=query.json(), index=self.table_name)['count']
        return count


@dataclass
class OpenSearchOutput(OutputIntegration, OpenSearchABC):
    """
        Class for handling data output operations to OpenSearch.
    """

    def __post_init__(self):
        """
            Calls the initialization method of the Mongo base class.
        """
        OpenSearchABC.__post_init__(self)
        self._truncate_index()

    def _truncate_index(self):
        """
            Deletes all documents in the index if overwriting is enabled.
        """
        if self.overwrite:
            query = ElasticQuery()
            query.query(ESQuery.match_all())

            response = self.session.delete_by_query(
                index=self.table_name,
                body=query.json()
            )
            print(f'The index: {self.table_name} has been deleted for overwriting.')
            print(f"Deleted {response['deleted']} rows")

    def set_data(self, data: List[dict]):
        """
            Inserts or updates data in OpenSearch.

            Args:
                data (List[dict]): The list of dictionaries containing data to be inserted or updated.
        """
        data = self._typecasting(self.schema, data)
        pk_col = ''
        for column_name, column_spec in self.schema.items():
            if column_spec.get('primary_key', False):
                pk_col = column_name
                break

        operations = []
        for doc in data:
            if pk_col:
                doc_id = doc[pk_col]
                action = {
                    '_op_type': 'update',
                    '_index': self.table_name,
                    '_id': doc_id,
                    'doc': doc,
                    'doc_as_upsert': True
                }
            else:
                action = {
                    '_op_type': 'index',
                    '_index': self.table_name,
                    '_source': doc
                }
            operations.append(action)

        result = helpers.bulk(self.session, operations)
        print('Operation result:', result[0])

    def filter_field_value(self):
        """
            Retrieves the maximum value of the filter field for the index.

            Returns:
                The maximum value of the filter field.
        """
        for column_name, column_spec in self.schema.items():
            if column_spec.get('filter_column', False):
                column_type = column_spec.get('type')
                res = self.session.search(
                    index=self.table_name,
                    body={
                        "aggs": {
                            "maxuid": {
                                "max": {"field": column_name}
                            }
                        }
                    }
                )
                result = res['aggregations']['maxuid'].get('value_as_string', None)
                conversion_function = arg_types_converting_methods.get(column_type.get_base_type())
                return conversion_function(result)
        return None
