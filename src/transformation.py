import inspect
import logging
import pickle
from dataclasses import dataclass
from enum import Enum
from typing import List, Dict, Type

from typing import Callable

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, Row, IntegerType, StringType

from data_types import DataType, ARRAY
from utils.constants import AIRFLOW_SPARK_DRIVER_PATH


class LoggerName(Enum):
    AIRFLOW_TASK = 'airflow.custom.global'


logger = logging.getLogger(LoggerName.AIRFLOW_TASK.value)


def create_spark_session(app_name: str, alloc_app_memory_gb: int = 8, alloc_app_cores: int = 8) -> SparkSession:
    threads_num = alloc_app_cores * 4

    spark = (SparkSession.builder
             .master("local[*]")
             .appName(app_name)
             .config("spark.driver.extraJavaOptions", "-Duser.timezone=GMT")
             .config("spark.executor.extraJavaOptions", "-Duser.timezone=GMT")
             .config("spark.sql.session.timeZone", "GMT")
             .config("spark.driver.memory", f"{alloc_app_memory_gb}g")
             .config("spark.default.parallelism", threads_num)
             .config("spark.sql.shuffle.partitions", threads_num)
             .config("spark.driver.maxResultSize", "2g")
             .config("spark.driver.extraClassPath", AIRFLOW_SPARK_DRIVER_PATH)
             .getOrCreate())
    logger.debug(f'SparkSession created {spark.builder._options}')
    return spark


@dataclass
class Transformation:
    """
        Represents a data transformation operation.

        Args:
            method (Callable): The transformation method to be applied.
            input_schema (Dict[str, Dict[str, Type[DataType]]]): The schema for the input data.
            output_schema (Dict[str, Dict[str, Type[DataType]]]): The schema for the output data.

        Raises:
            TypeError: If the transformation method does not have any parameters.

    """
    method: Callable
    input_schema: Dict[str, Dict[str, Type[DataType]]]
    output_schema: Dict[str, Dict[str, Type[DataType]]]
    table_name: str

    def _validate_method_incoming_args(self):
        """
            Validates whether the transformation method has at least one parameter.

            Raises:
                TypeError: If the transformation method does not have any parameters.
        """
        signature = inspect.signature(self.method)
        if not signature.parameters: raise TypeError(
            f"Incoming method {self.method} must receive at least one argument (DataFrame)")

    def __post_init__(self):
        self._validate_method_incoming_args()

    @staticmethod
    def validate_input_table(table: List[Dict[str, any]], schema: Dict[str, Dict[str, any]]) -> bool:
        """
        Validates whether the input table adheres to the specified schema.

        Args:
            table (List[Dict[str, any]]): The input table as a list of dictionaries.
            schema (Dict[str, Dict[str, any]]): The schema defining column names and types.

        Returns:
            bool: True if the table is valid according to the schema, False otherwise.
        """
        print("Validate input table")

        for row in table:
            for column, column_info in schema.items():
                if column not in row:
                    print(f"Column '{column}' is missing in the dictionary.")
                    return False

                column_type = column_info.get('type')
                if column_type is None:
                    print(f"Data type for column '{column}' is not defined in the schema.")
                    return False

                value = row[column]

                if isinstance(column_type, ARRAY):
                    if not isinstance(value, list):
                        print(f"Type mismatch for column '{column}'. Expected list, got '{type(value).__name__}'.")
                        return False

                    for item in value:
                        if not isinstance(item, column_type.element_type.get_base_type()):
                            print(
                                f"Type mismatch for elements in column '{column}'. Expected '{column_type.element_type.__name__}', "
                                f"got '{type(item).__name__}'.")
                            return False
                else:
                    if hasattr(column_type, 'get_base_type'):
                        if not isinstance(value, column_type.get_base_type()):
                            if value is None:
                                continue
                            print(
                                f"Type mismatch for column '{column}'. Expected type '{column_type.__name__}', got '{type(value).__name__}'.")
                            return False
                    else:
                        print(f"Type for column '{column}' does not have 'get_base_type' method.")
                        return False

        return True

    @staticmethod
    def validate_output_table(table: List[Dict[str, any]], schema: Dict[str, Dict[str, any]]) -> bool:
        """
        Validates whether the output table adheres to the specified schema.

        Args:
            table (List[Dict[str, any]]): The output table as a list of dictionaries.
            schema (Dict[str, Dict[str, any]]): The schema defining column names, types, and additional properties.

        Returns:
            bool: True if the table is valid according to the schema, False otherwise.
        """
        print("Validate output table")

        for row in table:
            for column, column_info in schema.items():
                if column not in row:
                    print(f"Column '{column}' is missing in the dictionary.")
                    return False

                datatype = column_info.get('type')
                if datatype is None:
                    print(f"Data type for column '{column}' is not defined in the schema.")
                    return False

                value = row[column]

                if isinstance(datatype, ARRAY):
                    if not isinstance(value, list):
                        print(f"Type mismatch for column '{column}'. Expected list, got '{type(value).__name__}'.")
                        return False

                    for item in value:
                        if not isinstance(item, datatype.element_type.get_base_type()):
                            print(
                                f"Type mismatch for elements in column '{column}'. Expected '{datatype.element_type.__name__}', "
                                f"got '{type(item).__name__}'.")
                            return False
                else:
                    if hasattr(datatype, 'get_base_type'):
                        if not isinstance(value, datatype.get_base_type()):
                            if value is None:
                                continue
                            print(
                                f"Type mismatch for column '{column}'. Expected type '{datatype.__name__}', got '{type(value).__name__}'.")
                            return False
                    else:
                        print(f"Data type for column '{column}' does not have 'get_base_type' method.")
                        return False

        return True

    def run(self, data: List[dict]):
        """
            Runs the data transformation process.

            Args:
                data (List[dict]): The input data to be transformed.

            Returns:
                List[dict]: The transformed output data.
        """
        print("run transform step")
        if self.validate_input_table(data, self.input_schema):
            df = self.convert_dict_to_spark_dataframe(data, self.input_schema)
            print("Run transform method")
            final_df = self.method(df)
            data = self.convert_spark_dataframe_to_dict(final_df)
        if self.validate_output_table(data, self.output_schema):
            return data

    def convert_dict_to_spark_dataframe(self, data: List[Dict[str, any]], input_schema: Dict[str, Dict[str, any]]):
        print('create test dataframe from unified sync')
        spark = create_spark_session(app_name=self.table_name)
        data_test = [
            (1, "Alice", 29),
            (2, "Bob", 35),
            (3, "Cathy", 25)
        ]
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)
        ])
        df = spark.createDataFrame(data_test, schema=schema)
        df.show()
        """
            Converts a list of dictionaries to a Spark DataFrame.

            Args:
                data (List[Dict[str, any]]): The input data as a list of dictionaries.
                input_schema (Dict[str, Dict[str, any]]): The schema defining column names and types.

            Returns:
                DataFrame: The Spark DataFrame.
        """
        print('Create spark session')
        spark_session = create_spark_session(app_name=self.table_name)
        print("Convert data dict to spark dataframe")
        struct_fields = []

        # Build the schema from input_schema
        for col in data[0].keys():
            column_type = input_schema[col].get('type')
            if isinstance(column_type, ARRAY):
                pyspark_type = column_type.pyspark_type
            else:
                pyspark_type = column_type.get_pyspark_type()

            struct_fields.append(StructField(col, pyspark_type, True))

        schema = StructType(struct_fields)
        print('data and schema')
        print(data)
        print(schema)
        try:
            pickle.dumps(data)
            pickle.dumps(schema)
            print("All objects are serializable!")
        except Exception as e:
            print(f"Serialization error: {e}")

        print('create empty rdd')
        emptyRDD = spark_session.sparkContext.emptyRDD()
        print(emptyRDD)

        print('create empty df')
        empty_df = spark_session.createDataFrame(emptyRDD, schema)
        empty_df.show()
        rows = [Row(**item) for item in data]
        new_df = spark_session.createDataFrame(rows, schema)

        result_df = empty_df.union(new_df)
        print('result df')
        result_df.show()
        return result_df

    @staticmethod
    def convert_spark_dataframe_to_dict(df):
        """
            Converts a Spark DataFrame to a list of dictionaries.

            Args:
                df: The Spark DataFrame.

            Returns:
                List[dict]: The converted list of dictionaries.
        """
        print("Convert spark dataframe to dictionary")
        columns = df.columns

        result = []
        for row in df.collect():
            row_dict = {}
            for col_name in columns:
                row_dict[col_name] = row[col_name]
            result.append(row_dict)

        return result
