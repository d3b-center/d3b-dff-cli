from cerberus import Validator
import warnings

# Suppress specific UserWarnings from Cerberus
warnings.filterwarnings("ignore", category=UserWarning, module="cerberus.validator")

class CustomValidator(Validator):
    def __init__(self, schema, rules=None, *args, **kwargs):
        """
        Initialize the CustomValidator with schema and optional custom rules.
        """
        super().__init__(schema, *args, **kwargs)
        self.custom_rules = rules or {}

    def _check_dependencies(self, field, document):
        """
        Check if the field's dependencies are met.
        """
        dependencies = self.schema.get(field, {}).get('dependencies', {})
        for dependency_field, allowed_values in dependencies.items():
            dependency_value = document.get(dependency_field)
            if isinstance(allowed_values, list):
                if dependency_value not in allowed_values:
                    return False
            else:
                if dependency_value != allowed_values:
                    return False
        return True

    def _validate_custom_rules(self, field, value):
        """
        Apply custom validation rules that are beyond the default Cerberus validation.
        """
        if field == 'file_name':
            file_format = self.document.get('file_format')
            if file_format:
                extensions = self.custom_rules.get('file_name_extensions')
                expected_extension = extensions.get(file_format)
                if expected_extension and not value.lower().endswith(expected_extension):
                    self._error(field, f"{field} must end with {expected_extension} for file_format '{file_format}'.")
                    return False

        if field == 'file_size':
            file_format = self.document.get('file_format')
            experiment = self.document.get("experiment_strategy")
            byte_cutoff_general = self.custom_rules.get('file_size_byte_cutoff').get('general_cutoff')
            byte_cutoff_wgs_wxs = self.custom_rules.get('file_size_byte_cutoff').get('wgs_wxs_cutoff')
            dependencies_format = self.custom_rules.get('file_size_byte_cutoff').get('dependencies').get('file_format')

            minum_value = byte_cutoff_wgs_wxs if experiment in ["wgs", "wxs", "wes"] else byte_cutoff_general

            if file_format in dependencies_format:
                if value < minum_value:
                    self._error(field, f"[Warning] must be at least {minum_value} for file_format '{file_format}'.")
                    return False

        return True

    
    def validate(self, document, *args, **kwargs):
        """
        Override validate method to first check dependencies, then apply default and custom validation.
        """
        self.document = document

        # Prepare filtered document with fields that meet dependencies
        filtered_document = {}
        for field in self.schema:
            if self._check_dependencies(field, document):
                filtered_document[field] = document.get(field)

        # Perform default validation
        super().validate(filtered_document, *args, **kwargs)

        for field, value in filtered_document.items():
            self._validate_custom_rules(field, value)
        
        # Filter and return errors
        print_errors = {field: errors for field, errors in self.errors.items() if field in filtered_document}

        # Determine overall validity based on errors
        is_valid = not bool(print_errors)

        return is_valid, print_errors