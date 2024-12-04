import json
from datetime import datetime, date

from abc import ABC
from dataclasses import dataclass
from typing import List, Dict

from pymongo import MongoClient, UpdateOne

from src.filter_manager import Filters
from src.intergrations.abstract_integrations import InputIntegration, OutputIntegration

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
class Mongo(ABC):
    """
        Base class for interacting with MongoDB, utilizing an abstract base class (ABC).

        Attributes:
            mongo_uri (str): The URI for connecting to MongoDB.
            dbname (str): The name of the database.
            table_name (str): The name of the collection.
    """
    mongo_uri: str
    dbname: str
    table_name: str

    def __post_init__(self):
        """
            Initializes the MongoDB client, database, and collection based on the provided parameters.
        """
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.dbname]
        self.collection = self.db[self.table_name]

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
class MongoInput(InputIntegration, Mongo):
    """
        Class for fetching data from MongoDB, inheriting from InputIntegration and Mongo.

        Attributes:
            filters (Filters): The Filters object for managing data filters.
    """
    filters: Filters = None

    def __post_init__(self):
        """
            Calls the initialization method of the Mongo base class.
        """
        Mongo.__post_init__(self)

    def get_sort_column(self):
        sort_columns_dict = {}
        multiply_sorted_columns = []
        for column_name, column_spec in self.schema.items():
            if column_spec.get('filter_column', False):
                sort_columns_dict['first_priority'] = [(column_name, 1)]
            if column_spec.get('primary_key', False):
                sort_columns_dict['second_priority'] = [(column_name, 1)]
            if column_spec.get('order_by', False):
                sort_columns_dict['third_priority'] = [(column_name, 1)]
            multiply_sorted_columns.append((column_name, 1))
        sort_columns_dict['fourth_priority'] = multiply_sorted_columns
        return sort_columns_dict.get('first_priority', sort_columns_dict.get('second_priority',
                                                                             sort_columns_dict.get('third_priority',
                                                                                                   sort_columns_dict.get(
                                                                                                       'fourth_priority'))))

    def get_data_from_mongodb(self, offset: int, limit: int, updated_at: datetime):
        """
        Fetches data from MongoDB considering offset, limit, and last updated time.

        Args:
            offset (int): The offset for pagination.
            limit (int): The limit on the number of results.
            updated_at (datetime): The last updated time for filtering data.

        Returns:
            List[Dict]: A list of dictionaries containing the fetched data.
        """
        projection = {**{column: 1 for column in self.schema.keys()}, **{'_id': 0}}
        filter_query = {}
        filter_column = self.get_sort_column()
        if updated_at:
            if filter_column:
                filter_query[filter_column[0][0]] = {'$gt': updated_at}
        filter_query = self.get_filter_query(filter_query)

        cursor = self.collection.find(filter_query or {}, projection).sort(filter_column).skip(
            offset * limit).limit(limit)

        data_list = []
        for document in cursor:
            data_list.append(document)
        return data_list

    def get_filter_query(self, filter_query):
        """
        Forms the MongoDB filter based on conditions defined in the filters object.

        Args:
            filter_query (dict): The initial filter query.

        Returns:
            dict: The updated filter query.
        """
        if self.filters:
            operator_mapping = {
                '>': '$gt',
                '<': '$lt',
                '>=': '$gte',
                '<=': '$lte',
                '!=': '$ne',
                '=': '$eq',
                'like': lambda val_: {'$regex': val_, '$options': ''},
                'ilike': lambda val_: {'$regex': val_, '$options': 'i'},
                'not_like': lambda val_: {'$not': {'$regex': val_, '$options': ''}},
                'not_ilike': lambda val_: {'$not': {'$regex': val_, '$options': 'i'}}
            }
            filter_conditions = self.filters.get_filter_conditions()

            for column, value in filter_conditions.items():
                if isinstance(value, dict):
                    mongo_filter = {}
                    for op, val in value.items():
                        if op in ['like', 'ilike', 'not_like', 'not_ilike']:
                            mongo_filter.update(operator_mapping[op](val))
                        else:
                            mongo_operator = operator_mapping[op]
                            mongo_filter[mongo_operator] = val
                    filter_query[column] = mongo_filter
                elif isinstance(value, list):
                    filter_query[column] = {'$in': value}
                else:
                    filter_query[column] = {'$eq': value}
        return filter_query

    def get_sync_length(self, max_filter_value):
        """
        Retrieves the number of documents matching the filter for synchronization.

        Args:
            max_filter_value: The maximum value for filtering.

        Returns:
            int: The number of matching documents.
        """
        filter_query = {}
        filter_query = self.get_filter_query(filter_query)
        if max_filter_value:
            for column_name, column_spec in self.schema.items():
                if column_spec.get('filter_column', False):
                    filter_query.setdefault(column_name, {})
                    filter_query[column_name]['$gt'] = max_filter_value
                    break
        return self.collection.count_documents(filter_query)

    def get_data(self, offset: int, limit: int, updated_at: datetime) -> List[dict]:
        """
        Fetches and typecasts data from MongoDB.

        Args:
            offset (int): The offset for pagination.
            limit (int): The limit on the number of results.
            updated_at (datetime): The last updated time for filtering data.

        Returns:
            List[dict]: A list of dictionaries with typecasted data.
        """
        print('Get data from mongodb')
        data_dict = self.get_data_from_mongodb(offset, limit, updated_at)
        print('Typecasting data dict')
        converted_data = self._typecasting(self.schema, data_dict)
        return converted_data


@dataclass
class MongoOutput(OutputIntegration, Mongo):
    """
    Class for writing data to MongoDB, inheriting from OutputIntegration and Mongo.
    """

    def __post_init__(self):
        """
            Calls the initialization method of the Mongo base class.
        """
        Mongo.__post_init__(self)
        self._truncate_collection()

    def _truncate_collection(self):
        if self.overwrite:
            result = self.collection.delete_many({})
            print(f'The collection: {self.table_name} has been deleted for overwriting.')
            print(f"Deleted {result.raw_result['n']} rows")

    def set_data(self, data: List[dict]):
        data = self._typecasting(self.schema, data)
        pk_col = ''
        for column_name, column_spec in self.schema.items():
            if column_spec.get('primary_key', False):
                pk_col = column_name
                break
        if pk_col:
            operations = []
            for doc in data:
                filter_criteria = {pk_col: doc[pk_col]}
                update_data = {"$set": doc}
                operations.append(UpdateOne(filter_criteria, update_data, upsert=True))
            result = self.collection.bulk_write(operations, ordered=False).upserted_count
        else:
            result = self.collection.insert_many(data).inserted_ids
        print('Inserted document IDs:', result)

    def filter_field_value(self):
        """
            Retrieves the maximum value of the filter field for the table.

            Returns:
                The maximum value of the filter field.
        """
        for column_name, column_spec in self.schema.items():
            if column_spec.get('filter_column', False):
                column_type = column_spec.get('type')
                max_document = self.collection.find_one(sort=[(column_name, -1)])
                max_value = max_document[column_name] if max_document else None
                conversion_function = arg_types_converting_methods.get(column_type.get_base_type())
                return conversion_function(max_value)
        return None
