# Unified Sync

This project is designed to simplify the process of building ETL (Extract, Transform, Load) pipelines between various
data sources. To create a new ETL, you need to select the integrations you intend to work with and define a
straightforward configuration. All integrations are backward-compatible, ensuring that updates won't disrupt existing
configurations. You can set up bidirectional ETL processes. This project can be imported as a submodule into your main
project.

## Installation

Clone this repository into your project and install it using the command:

```
pip install -e unified_sync
```

## Get Started

To get started, create a class for your table derived from the abstract class TableIntegrationClass, specifying the
names of the source and target tables. Define methods to configure field names, types, and additional behavior.

### Example of a Table Class:

```
class YourClassName(TableIntegrationClass):
    table_name = 'schema_name.table_name'
    table_name_source = 'schema_name.table_name'
    source_db_name = 'source_dbname'
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
```

### Supported Filters:

Available filtering options include:

* Comparison operators: >, <, >=, <=, !=, =.
* Pattern matching: like, ilike, not_like, not_ilike.
* Filtering by list values:

```
 def filters():
        return {'uuid': ['id_1', 'id_2', 'id_3', ...]}
```

## Using Unified Sync: Entry Point Script

To use the Unified Sync module, your project should include an entry-point script. This script configures database
connections, selects integrations, and executes the synchronization process.

Below is a description of the main function used to achieve this.

### Example Main Function

```
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
```

### main Function Overview

The main function handles the following tasks:

* Initialize Configuration Parameters:
    - Define connection settings for the source and target databases.

* Support Optional Features:
    - Filters, force prefetching, and overwrite behaviors are customizable.

* Handle Transformations:
    - Apply a transformation to data if defined in the table class.

* Execute Data Synchronization:
    - Use the sync function to transfer data from source to target.

### Supported Integrations

The main function is compatible with the following input and output integrations:

* PostgreSQL Integrations
    - PostgresqlInput
        - Fetches data from a PostgreSQL table.
    - PostgresqlOutput
        - Loads data into a PostgreSQL table.
* MongoDB Integrations
    - MongoInput
        - Fetches data from a MongoDB collection.
    - MongoOutput
        - Loads data into a MongoDB collection.
* OpenSearch Integrations
    - OpenSearchInput
        - Fetches data from an OpenSearch index.
    - OpenSearchOutput
        - Loads data into an OpenSearch index.

#### To switch between integrations, modify the initialization logic for the input and output classes.

### Code Example

Here’s how the synchronization process works step by step:

1. Update Source Configuration
   If the table object has a source_db_name, the config_dict_source is updated with it.
2. Prepare Integration Parameters
   Create dictionaries (postgres_input_kwargs and postgres_output_kwargs) with connection details and table schemas.
3. Optional Features

    * If filters are defined, add them to the input configuration.
    * If force_prefetching is enabled, update the output configuration.
    * If overwrite is enabled, set it in the output configuration.
4. Create Integration Classes
   Instantiate the input and output classes using the prepared parameters:

> postgres_input_class = PostgresqlInput(**postgres_input_kwargs)
> postgres_output_class = PostgresqlOutput(**postgres_output_kwargs)

5. Handle Transformations
   If the table class defines a transform method, create a Transformation object. Add it to the synchronization
   configuration:

> transformation = Transformation(
> method=table_obj.transform,
> input_schema=table_obj.schema_input(),
> output_schema=table_obj.schema_output(),
> table_name=table_obj.table_name
)
> config = Config(input=postgres_input_class, transform=transformation, output=postgres_output_class)

6. Run Synchronization
   Pass the configuration to the sync function to execute the process:

> sync(config)

#### This structure ensures flexibility, allowing you to use the same function with different integrations or customize behavior based on table-specific requirements.