import json
import argparse
import csv

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
                
                cell_value = str(row.get(col)).lower()

                if op_value != "" and op_value is not None:
                    allowed_values = op_value.split(",")
                    if len(allowed_values) > 1:
                        if cell_value not in map(str.lower,allowed_values):
                            error_messages.append(f"*{col}*: must be one of {', '.join(allowed_values)}.")
                    else:
                        if cell_value != op_value.lower():
                            error_messages.append(f"*{col}*: must be {op_value}.")

                if is_empty and not cell_value:
                    error_messages.append(f"*{col}*: cannot be empty.")
                
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
                        try:
                            file_size_in_gb = float(row.get(col, 0)) / (1024 * 1024 * 1024)  # Convert to GB
                            if file_size_in_gb <= float(greater_than_value.rstrip(" GB")):
                                error_messages.append(f"Warning: *{col}* less than {greater_than_value}")
                        except ValueError:
                            error_messages.append(f"*{col}* is not a valid numeric value")



    if error_messages:
        return False, error_messages  # Return all error messages for this row
    else:
        return True, None

def main(args):
    rule_type = args.rule_type
    rules_json = args.rules
    manifest_data = []
    with open(args.manifest_file, "r") as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            manifest_data.append(row)
            
    with open(rules_json, "r") as json_file:
        validation_rules = json.load(json_file)[rule_type]

    # Iterate through each row in the DataFrame and perform validation
    validation_failed = False
    for index, row in enumerate(manifest_data):
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