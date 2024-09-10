from cerberus import Validator
import pandas as pd
import warnings

# Suppress specific UserWarnings from Cerberus
warnings.filterwarnings("ignore", category=UserWarning, module="cerberus.validator")
class CustomValidator(Validator):
    def __init__(self, schema, rules=None, *args, **kwargs):
        super().__init__(schema, *args, **kwargs)
        self.rules = rules if rules else {}

    def _validate_dependencies(self, dependencies, field, value):
        """
        Check if the field's dependencies are met. If not, skip validation for this field.
        """
        for dependency_field, allowed_values in dependencies.items():
            dependency_value = self.document.get(dependency_field)

            # Check if the allowed_values is a list or a single value
            if isinstance(allowed_values, list):
                if dependency_value not in allowed_values:
                    return False  # Dependencies not met
            else:
                if dependency_value != allowed_values:
                    return False  # Dependencies not met
        
        # Dependencies are met, perform validation
        return True

    def _validate_field(self, field, value, field_schema):
        """
        Validate the field value based on its schema.
        """
        field_type = field_schema.get('type')
        allowed_values = field_schema.get('allowed')

        # Validate required fields
        if field_schema.get('required') and (value is None or pd.isna(value) or value == ''):
            self._error(field, f"{field} is required.")
            return False

        # Handle validation for allowed values
        if allowed_values and value not in allowed_values:
            self._error(field, f"{field} must be one of {allowed_values}.")
            return False
        
        # Validate type
        if field_type:
            if field_type == 'boolean':
                if value not in ['true', 'false']:
                    self._error(field, f"{field} must be a boolean value.")
                    return False
            elif field_type == 'integer':
                if not (isinstance(value, int) or (isinstance(value, float) and value.is_integer())):
                    self._error(field, f"{field} must be of integer type.")
                    return False
            elif field_type == 'string':
                if not isinstance(value, str):
                    self._error(field, f"{field} must be of string type.")
                    return False
                
        # Apply additional validation based on custom rules
        if field == 'file_name':
            file_format = self.document.get('file_format')
            if file_format:
                extensions = self.rules.get('file_name_extensions')
                expected_extension = extensions.get(file_format)
                if expected_extension and not value.lower().endswith(expected_extension):
                    self._error(field, f"{field} must end with {expected_extension} for file_format '{file_format}'.")
                    return False

        if field == 'file_size':
            file_format = self.document.get('file_format')
            experiment = self.document.get("experiment_strategy")
            
            byte_cutoff_general = self.rules.get('file_size_byte_cutoff').get('general_cutoff')
            byte_cutoff_wgs_wxs = self.rules.get('file_size_byte_cutoff').get('wgs_wxs_cutoff')
            dependencies_format = self.rules.get('file_size_byte_cutoff').get('dependencies').get('file_format')

            if experiment in ["wgs", "wxs", "wes"]:
                minum_value = byte_cutoff_wgs_wxs
            else:
                minum_value = byte_cutoff_general
            
            if file_format in dependencies_format:
                if value < minum_value:
                    self._error(field, f"Warning: *{field}* must be at least {minum_value} for file_format '{file_format}'.")
                    return False
        return True

    def validate(self, document, *args, **kwargs):
        """
        Override validate method to ensure dependencies are respected.
        """
        self.document = document
        is_valid = True
        
        for field, field_schema in self.schema.items():
            value = document.get(field)
            dependencies = field_schema.get('dependencies')
            
            if dependencies:
                if not self._validate_dependencies(dependencies, field, value):
                    continue  # Skip validation if dependencies are not met

            # Perform validation if dependencies are met
            if not self._validate_field(field, value, field_schema):
                is_valid = False
        
        return is_valid