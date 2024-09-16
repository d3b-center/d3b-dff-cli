import argparse
import pandas as pd
import json
import os
from .cerberus_custom_checks import CustomValidator

wk_dir = os.path.dirname(os.path.abspath(__file__))
validation_schema = os.path.join(wk_dir, "validation_rules_schema.json")

def load_data(manifest_file):
    """
    Load data from a manifest file and convert strings to lowercase.
    """
    file_extension = manifest_file.split('.')[-1].lower()
    if file_extension == 'csv':
        manifest_data = pd.read_csv(manifest_file)
    elif file_extension == 'tsv':
        manifest_data = pd.read_csv(manifest_file, delimiter='\t')
    elif file_extension in ['xls', 'xlsx']:
        xlsx = pd.ExcelFile(manifest_file)
        if len(xlsx.sheet_names) == 1:
            manifest_data = pd.read_excel(xlsx)
        elif "Genomics_Manifest" in xlsx.sheet_names:
            manifest_data = pd.read_excel(xlsx, sheet_name="Genomics_Manifest")
        else:
            raise ValueError(f"Sheet 'Genomics_Manifest' not found in {manifest_file}")
    else:
        raise ValueError("Unsupported file format. Please provide a CSV, TSV, or Excel file.")
    
    manifest_data = manifest_data.apply(lambda col: col.astype(str).str.lower() if col.dtype.name in ['object'] else col)
    return manifest_data

def convert_schema_to_lowercase(schema):
    """
    Convert all string values in the schema to lowercase.
    """
    for k, v in schema.items():
        if isinstance(v, dict):
            convert_schema_to_lowercase(v)
        elif isinstance(v, list):
            schema[k] = [item.lower() if isinstance(item, str) else item for item in v]
        elif isinstance(v, str):
            schema[k] = v.lower()
    return schema

def validate_data(df, schema_json):
    """
    Validate the DataFrame against the schema.
    """
    valid = True
    errors = []
    custom_rules = schema_json.get('custom_rules', {})
    
    for index, row in df.iterrows():
        experiment_strategy = row.get("experiment_strategy", "").lower()
        platform = row.get("platform", "").lower()
        
        if platform == "pacbio":
            rule_type = "pacbio_longread_rules"
        else:
            if experiment_strategy in ["wgs", "wxs", "wes", "target sequencing", "panel", "target"]:
                rule_type = "DNAseq_rules"
            elif experiment_strategy in ["rna-seq", "rnaseq", "mirna-seq", "mirnaseq"]:
                rule_type = "RNAseq_rules"
            elif experiment_strategy in ["scrna-seq", "snran-seq", "scrnaseq", "snranseq"]:
                rule_type = "single_cell_rules"
            elif experiment_strategy in ["methtlation", "methylation microarray"]:
                rule_type = "methylation_rules"
            else:
                raise ValueError(f"Unsupported experiment_strategy for Row {index + 1}")
        
        schema = schema_json.get(rule_type, {})
        
        # Filter out fields not in the schema fields for combined manifest
        row_dict = row.to_dict()
        filtered_row_dict = {k: v for k, v in row_dict.items() if k in schema}
        
        v = CustomValidator(schema, custom_rules)
        (is_valid, out_error) = v.validate(filtered_row_dict)
        
        if not is_valid:
            valid = False
            errors.append({
                'row': index + 1,
                'errors': out_error
            })
    
    return valid, errors

def main(args):
    """
    Main function to load schema, validate data, and print the validation report.
    """
    with open(validation_schema, 'r') as f:
        schema = json.load(f)
    
    schema_json = convert_schema_to_lowercase(schema)

    # Load and preprocess the data
    df = load_data(args.manifest_file)

    # Validate the data
    valid, errors = validate_data(df, schema_json)

    # Print validation report
    if valid:
        print("====Validation Passed====\n  All rows are valid.")
    else:
        # Check if all warning messages
        only_warnings = all("Warning" in field_error for error in errors for field_errors in error['errors'].values() for field_error in field_errors)

        if only_warnings:
            print("====Validation Warnings====")
        else:
            print("====Validation Failed====")
        
        for error in errors:
            print(f"Row {error['row']}:")
            for field, field_errors in error['errors'].items():
                for field_error in field_errors:
                    print(f"  {field}: {field_error}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate a manifest based on defined rules.")
    parser.add_argument("-manifest_file", required=True, help="Path to the manifest file (CSV/Excel).")
    args = parser.parse_args()
    main(args)