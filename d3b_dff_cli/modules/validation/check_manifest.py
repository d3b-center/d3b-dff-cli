import json
import argparse
import pandas as pd
import re

# Define a function to perform validation
def validate_row(row, rules):
    error_messages = []
    for rule in rules:
        conditions = rule.get("conditions", [])
        consequences = rule.get("consequences", [])
        condition_met = all(
            str(row.get(cond["column"])).lower() in map(str.lower,cond.get("equals").split(","))
            for cond in conditions
        )
        if condition_met:
            for consequence in consequences:
                col = consequence.get("column")
                op_value = consequence.get("equals")
                is_empty = consequence.get("empty")
                
                cell_value = row.get(col)
                if is_empty and pd.isna(cell_value):
                    error_messages.append(f"*{col}*: cannot be empty.")
                else:
                    cell_value = str(cell_value).lower()

                if op_value != "" and op_value is not None:
                    allowed_values = op_value.split(",")
                    if len(allowed_values) > 1:
                        if cell_value not in map(str.lower,allowed_values):
                            error_messages.append(f"*{col}*: must be one of {', '.join(allowed_values)}.")
                    else:
                        if cell_value != op_value.lower():
                            error_messages.append(f"*{col}*: must be {op_value}.")

                # Check if file_name ends with a valid extension
                if col == "file_name" and "ends_with" in consequence:
                    format = conditions[0].get("equals")
                    valid_extensions = consequence["ends_with"].split(",")
                    if not any(cell_value.lower().endswith(ext.strip()) for ext in valid_extensions):
                        error_messages.append(f"*file_format* is: {format}, but *{col}* is: {cell_value}, which must end with: {', '.join(valid_extensions)}")
                
                # Check if file_format is "FASTQ," "BAM," or "CRAM" and file_size > specified value
                if col == "file_size" and row.get("file_format", "").lower() in ["fastq", "bam", "cram"]:
                    greater_than_value = consequence.get("greater_than")
                    if greater_than_value:
                        experiment = row.get("experiment_strategy", "").lower()
                        if experiment in ["wgs", "wxs", "wes"]:
                            greater_than_value = "1 GB"
                            minum_value = 1_000_000_000 # WGS/WXS should be greater than 1G
                        else:
                            greater_than_value = consequence.get("greater_than")
                            minum_value = float(greater_than_value.rstrip("M"))*1000_000 # Other experimental strategy should be greater than the specified value.
                        
                        if pd.notna(cell_value):
                            try:
                                size_byte = float(cell_value)
                                if size_byte < minum_value:
                                    error_messages.append(f"Warning: *{col}* less than {greater_than_value}")

                            except ValueError:
                                error_messages.append(f"*{col}*: {cell_value} is not a valid value")

    if error_messages:
        return False, error_messages  # Return all error messages for this row
    else:
        return True, None

def main(args):
    rule_type = args.rule_type
    rules_json = args.rules
    manifest = args.manifest_file

    file_extension = manifest.split('.')[-1].lower()
    if file_extension == 'csv':
        manifest_data = pd.read_csv(manifest)
    elif file_extension == 'tsv':
        manifest_data = pd.read_csv(manifest, delimiter='\t')
    elif file_extension in ['xls', 'xlsx']:
        xlsx = pd.ExcelFile(manifest)
        if len(xlsx.sheet_names) == 1:
            manifest_data = pd.read_excel(xlsx)
        elif "Genomics_Manifest" in xlsx.sheet_names:
            manifest_data = pd.read_excel(xlsx, "Genomics_Manifest")
        else:
            raise ValueError(f"Genomics_Manifest sheet not found in {manifest}")
    else:
        raise ValueError("Unsupported file format. Please provide a CSV, TSV, or Excel file.")
    
    with open(rules_json, "r") as json_file:
        validation_rules = json.load(json_file)[rule_type]

    # Iterate through each row in the DataFrame and perform validation
    validation_failed = False
    for index, row in manifest_data.iterrows():
        is_valid, messages = validate_row(row, validation_rules)
        if not is_valid:
            error_message = "Validation Failed For Row {0}:\n{1}".format(index + 1, '\n'.join(messages))
            print(error_message,"\n")
            validation_failed = True
    if not validation_failed:
        print("Validation Passed: All rows are valid.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate a manifest based on defined rules.")
    parser.add_argument("-rules", help="Formatted JSON file defining validation rules.", required=True)
    parser.add_argument("-rule_type", help="Specific type of validation rule defined in the json rule file.")
    parser.add_argument("-manifest_file", help="Manifest based on the d3b genomics manifest template.")
    args = parser.parse_args()
    main(args)