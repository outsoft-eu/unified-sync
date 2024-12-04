import logging

from pyspark.sql.functions import lit, concat, col

from config import Config
from data_types import Integer, String, DateTime
from filter_manager import Filters
from src.intergrations.abstract_integrations import TableIntegrationClass
from src.intergrations.postgresql import PostgresqlInput, PostgresqlOutput
from processing import sync
from transformation import LoggerName, Transformation


class UnifiedOperator(TableIntegrationClass):
    table_name = 'stg_tables.operator'
    table_name_source = 'public.operator'
    source_db_name = 'operator'
    force_prefetching = False
    overwrite = False

    @staticmethod
    def schema_input():
        return {
            'id': {'type': Integer, 'primary_key': True},
            'uuid': {'type': String},
            'first_name': {'type': String},
            'last_name': {'type': String},
            'phone_number': {'type': String},
            'email': {'type': String},
            'country': {'type': String},
            'registration_date': {'type': DateTime(with_timezone=False)},
            'status': {'type': String},
            'status_change_date': {'type': DateTime(with_timezone=False)},
            'status_change_author': {'type': String},
            'registered_by': {'type': String},
            'status_reason': {'type': String},
            'operator_role': {'type': String},
            'updated_at': {'type': DateTime(with_timezone=False), 'filter_column': True},
            'created_at': {'type': DateTime(with_timezone=False)}
        }

    def schema_output(self):
        schema = self.schema_input()
        schema['uuid_compound'] = {'type': String}
        schema['prod_num'] = {'type': Integer}
        return schema

    @staticmethod
    def transform(df):
        df = df.select(*df.columns) \
            .withColumn('prod_num', lit(1)) \
            .withColumn('uuid_compound', concat(col('uuid'), lit('-'), col('prod_num')))
        return df

    @staticmethod
    def filters():
        return {'uuid': {'not_ilike': '%test'}}


logger = logging.getLogger(LoggerName.AIRFLOW_TASK.value)


def main(config_dict_dwh: dict,
         config_dict_source: dict,
         table_class):
    table_obj = table_class()
    if hasattr(table_obj, 'source_db_name') and table_obj.source_db_name:
        config_dict_source.update({'dbname': table_obj.source_db_name})
    postgres_input_kwargs = {
        'host': config_dict_source['host'],
        'user': config_dict_source['user'],
        'password': config_dict_source['password'],
        'port': config_dict_source['port'],
        'dbname': config_dict_source['dbname'],
        'table_name': table_obj.table_name_source,
        'schema': table_obj.schema_input()
    }
    postgres_output_kwargs = {
        'host': config_dict_dwh['host'],
        'user': config_dict_dwh['user'],
        'password': config_dict_dwh['password'],
        'port': config_dict_dwh['port'],
        'dbname': config_dict_dwh['dbname'],
        'table_name': table_obj.table_name,
        'schema': table_obj.schema_output()
    }
    if hasattr(table_obj, 'filters') and callable(getattr(table_obj, 'filters')):
        filters = Filters(filter_dict=table_obj.filters(),
                          schema=table_obj.schema_input())
        postgres_input_kwargs['filters'] = filters

    if hasattr(table_obj, 'force_prefetching') and table_obj.force_prefetching:
        postgres_output_kwargs['force_prefetching'] = True

    if hasattr(table_obj, 'overwrite') and table_obj.overwrite:
        postgres_output_kwargs['overwrite'] = True

    postgres_input_class = PostgresqlInput(**postgres_input_kwargs)
    postgres_output_class = PostgresqlOutput(**postgres_output_kwargs)

    if hasattr(table_obj, 'transform'):
        transformation = Transformation(method=table_obj.transform, input_schema=table_obj.schema_input(),
                                        output_schema=table_obj.schema_output(), table_name=table_obj.table_name)
        config = Config(input=postgres_input_class, transform=transformation, output=postgres_output_class)
    else:
        config = Config(input=postgres_input_class, output=postgres_output_class)

    sync(config)
