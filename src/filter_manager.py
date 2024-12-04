from dataclasses import dataclass
from typing import Dict

from src.intergrations.abstract_integrations import Schemable


@dataclass
class Filters(Schemable):
    """
    Validates and typecasts filter values based on the provided schema.

    Args:
        filter_dict (Dict): A dictionary containing filter data to be validated and typecasted according to the schema.

    Raises:
        KeyError: If any key in the filter dictionary does not belong to the schema.
        TypeError: If any value in the filter dictionary cannot be cast to the corresponding base type defined in the schema.

    Returns:
        Dict: The filter dictionary with validated and typecasted values according to the schema.

    """
    filter_dict: Dict

    def __post_init__(self):
        self._validate_filters_column()

    def _validate_filters_column(self):
        """
            Validates each key-value pair in the filter dictionary against the schema.

            Raises:
                KeyError: If any key in the filter dictionary does not belong to the schema.
                TypeError: If any value in the filter dictionary cannot be cast to the corresponding base type defined in the schema.
        """
        for key, value in self.filter_dict.items():
            if key in self.schema:
                schema_type = self.schema[key]['type'].base_type
                if isinstance(value, list):
                    for v in value:
                        if not isinstance(v, schema_type):
                            raise TypeError(f"Value {v} for key {key} cannot be cast to base type {schema_type}")
                elif isinstance(value, dict):
                    for operator, val in value.items():
                        if not isinstance(val, schema_type):
                            raise TypeError(f"Value {val} for key {key} cannot be cast to base type {schema_type}")
                else:
                    if not isinstance(value, schema_type):
                        raise TypeError(f"Value {value} for key {key} cannot be cast to base type {schema_type}")
            else:
                raise KeyError(f"Key {key} in filter_dict does not belong to the schema")

    def get_filter_conditions(self):
        """
            Returns the filter conditions after validation and typecasting.

            Returns:
                Dict: The filter dictionary with validated and typecasted values according to the schema.
        """
        return self.filter_dict
