import pandas as pd
import json
import black
from kf_lib_data_ingest.common import constants
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-sample", help="sample metadata manifest(.csv)", required=False)
parser.add_argument("-genomic", help="genomics file metadata manifest(.csv)", required=False)
parser.add_argument("-hash", help="hash report manifest(.csv)", required=False)
parser.add_argument("-mapping", help="ingest mapping file(.json)", required=False)
args = parser.parse_args()

# Initialize DataFrames
sample_df = gf_df = hash_df = None

# Load input files
def load_file(filepath):
    if filepath.endswith('.tsv'):
        delimiter = '\t'
    else:
        delimiter = ','
    return pd.read_csv(filepath, delimiter=delimiter)

if args.sample and not args.genomic:
    sample_df = load_file(args.sample)
    use_case = 'sample_only'
elif args.genomic and not args.sample:
    gf_df = load_file(args.genomic)
    hash_df = load_file(args.hash)
    use_case = 'genomic_only'
elif args.sample and args.genomic:
    sample_df = load_file(args.sample)
    gf_df = load_file(args.genomic)
    hash_df = load_file(args.hash)
    use_case = 'both'
else:
    raise ValueError("Either 'sample' or 'genomic' (or both) must be provided.")

mapping_df = args.mapping
with open(mapping_df, 'r') as json_file:
    mappings = json.load(json_file)

######## check study_id
def check_study_id(df):
    study_values = df['study_id'].dropna().unique()
    if len(study_values) == 1:
        study_str = str(study_values[0])
        return study_str
    else:
        print(f"ERROR: Multiple studies detected {study_values}. Please ensure that only one study is being processed at a time")
        sys.exit()

######## Check minimum required fields
# Required columns
required_fields = {
    'sample_manifest': ["study_id","subject_id","sample_id","aliquot_id","tissue_type","analyte_type","visible","visibility_reason"],
    'genomics_manifest': ["aliquot_id","kf_sequence_center_id","sequencing_id","experiment_strategy","file_path","visible","visibility_reason"],
    'harmonized_manifest': ["biospecimen_id","file_path","sequencing_experiment_id","visible","visibility_reason"],
    'hash_manifest': ["uri","sizeInBytes"]
}

# Check for missing columns
def check_required_columns(df, df_name):
    required_columns = required_fields[df_name]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"ERROR: Required columns are missing in {df_name}: {missing_columns}")
        return True

    nan_columns = [col for col in required_columns if df[col].isnull().any()]
    if nan_columns:
        print(f"ERROR: Required columns with Null in {df_name}: {nan_columns}")
        return True
    return False

missing_columns = False
if use_case == 'sample_only':
    missing_columns |= check_required_columns(sample_df, 'sample_manifest')
    study_str = check_study_id(sample_df)

elif use_case == 'genomic_only':
    missing_columns |= check_required_columns(gf_df, 'harmonized_manifest')
    missing_columns |= check_required_columns(hash_df, 'hash_manifest')
    study_str = check_study_id(gf_df)

elif use_case == 'both':
    missing_columns |= check_required_columns(sample_df, 'sample_manifest')
    missing_columns |= check_required_columns(gf_df, 'genomics_manifest')
    missing_columns |= check_required_columns(hash_df, 'hash_manifest')
    study_str = check_study_id(sample_df)

# Exit if any columns are missing
if missing_columns:
    sys.exit()

## Define templates
source_sample_template = """
import os
import re
import pandas as pd
from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.constants import COMMON, GENOMIC_FILE
from kf_lib_data_ingest.common.pandas_utils import Split
from kf_lib_data_ingest.etl.extract.operations import (
    constant_map,
    keep_map,
    row_map,
    value_map,
)

source_data_url = "file://../data/sample_manifest.csv"

operations = [
{operations}
]

"""

source_genomics_template = """
import os
import re
import pandas as pd
from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.constants import COMMON, GENOMIC_FILE
from kf_lib_data_ingest.common.pandas_utils import Split
from kf_lib_data_ingest.etl.extract.operations import (
    constant_map,
    keep_map,
    row_map,
    value_map,
)

source_data_url = "file://../data/genomics_file_manifest.csv"

FILE_EXT_FORMAT_MAP = {{
    "BAI": GENOMIC_FILE.FORMAT.BAI,
    "BAM": GENOMIC_FILE.FORMAT.BAM,
    "CRAI": GENOMIC_FILE.FORMAT.CRAI,
    "CRAM": GENOMIC_FILE.FORMAT.CRAM,
    "DCM": GENOMIC_FILE.FORMAT.DCM,
    "FASTQ": GENOMIC_FILE.FORMAT.FASTQ,
    "GPR": GENOMIC_FILE.FORMAT.GPR,
    "VCF": GENOMIC_FILE.FORMAT.VCF,
    "IDAT": GENOMIC_FILE.FORMAT.IDAT,
    "PDF": GENOMIC_FILE.FORMAT.PDF,
    "SVS": GENOMIC_FILE.FORMAT.SVS,
    "TBI": GENOMIC_FILE.FORMAT.TBI,
    "HTML": GENOMIC_FILE.FORMAT.HTML,
    "MAF": GENOMIC_FILE.FORMAT.MAF,
    "CNS": "cns",
    "TXT": "txt",
    "PNG": "png",
    "CSV": "csv",
    "PED": "ped",
    "SEG": "seg",
    "TAR": "tar",
    "TSV": "tsv",
}}

def file_format(x):
    if x in FILE_EXT_FORMAT_MAP:
        file_ext = FILE_EXT_FORMAT_MAP[x]
    else:
        file_ext = None
    return file_ext

def fname(key):
    return key.rsplit("/", 1)[-1]

operations = [
{operations}
]

"""

source_hash_template = """
from kf_lib_data_ingest.common import constants  # noqa F401
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.etl.extract.operations import value_map, row_map, keep_map

source_data_url = "file://../data/hash_manifest.csv"

def fname(key):
    return key.rsplit("/", 1)[-1]

operations = [
{operations}
]

"""

package_config_template = """
# The list of entities that will be loaded into the target service. These
# should be class_name values of your target API config's target entity
# classes.

target_service_entities = [
{operations}
]

# All paths are relative to the directory this file is in
extract_config_dir = "extract_configs"

transform_function_path = "transform_module.py"

# TODO - Replace this with your own unique identifier for the project. This
# will become CONCEPT.PROJECT.ID during the Load stage.
project = "{study}"
"""

transform_module_template = """
from kf_lib_data_ingest.common.concept_schema import CONCEPT

# Use these merge funcs, not pandas.merge
from kf_lib_data_ingest.common.pandas_utils import (
    merge_wo_duplicates,
    outer_merge,
)
from kf_lib_data_ingest.config import DEFAULT_KEY


def transform_function(mapped_df_dict):

    df_genomic = merge_wo_duplicates(
        mapped_df_dict['config_genomic_file.py'],
        mapped_df_dict['config_sample.py'],
        on=CONCEPT.BIOSPECIMEN.ID,
        how="left",
    )

    df = merge_wo_duplicates(
        df_genomic,
        mapped_df_dict['config_hash.py'],
        on=CONCEPT.GENOMIC_FILE.ID,
        how="left",
    )

    return {DEFAULT_KEY: df}

"""

transform_module_template_sample = """
from kf_lib_data_ingest.common.concept_schema import CONCEPT

# Use these merge funcs, not pandas.merge
from kf_lib_data_ingest.common.pandas_utils import (
    merge_wo_duplicates,
    outer_merge,
)
from kf_lib_data_ingest.config import DEFAULT_KEY


def transform_function(mapped_df_dict):

    df = mapped_df_dict['config_sample.py']

    return {DEFAULT_KEY: df}

"""

transform_module_template_genomic = """
from kf_lib_data_ingest.common.concept_schema import CONCEPT

# Use these merge funcs, not pandas.merge
from kf_lib_data_ingest.common.pandas_utils import (
    merge_wo_duplicates,
    outer_merge,
)
from kf_lib_data_ingest.config import DEFAULT_KEY


def transform_function(mapped_df_dict):

    df = merge_wo_duplicates(
        mapped_df_dict['config_genomic_file.py'],
        mapped_df_dict['config_hash.py'],
        on=CONCEPT.GENOMIC_FILE.ID,
        how="left"
    )
    return {DEFAULT_KEY: df}

"""

## create extract_configs scripts

def generate_operations_script(df, mappings, mapping_section):
    operations = []

    if mapping_section in mappings:
        section = mappings[mapping_section]
        for mapping in section:
            in_col = mapping.get('in_col')
            out_col = mapping.get('out_col')
            map_type = mapping.get('type')
            m = mapping.get('m')
        
            if map_type == 'row_map' and out_col == 'CONCEPT.GENOMIC_FILE.HASH_DICT':
                hash_map = mapping.get('m')
                hash_columns_present = [col for col in hash_map.keys() if col in df.columns]
                hash_type = []
                for col in hash_columns_present:
                    hash = hash_map[col]
                    hash_type.append(hash)
                hash_dict = ",\n".join(hash_type)
                operations.append(f'row_map(out_col={out_col}, m=lambda x:{{{hash_dict}}})')
            elif in_col and in_col in df.columns and (not df[in_col].isna().all()):
                if map_type == 'keep_map':
                    operations.append(f'keep_map(in_col="{in_col}", out_col={out_col})')
                elif map_type == 'value_map':
                    operations.append(f'value_map(in_col="{in_col}", m={m}, out_col={out_col})')
            elif map_type == 'constant_map':
                operations.append(f'constant_map(m={m}, out_col={out_col})')

    operations_script = "    " + ",\n    ".join(operations) + "\n"
    return operations_script

def save_script(source_template, operations_script, scriptname):
    script_content = source_template.format(operations=operations_script)

    # Format the script using black
    formatted_script = black.format_str(script_content, mode=black.FileMode())
    
    with open(scriptname, 'w') as f:
        f.write(formatted_script)

## create ingest_package_config.py and transform_module.py
def generate_ingest_config_sample(study,operations_script,mappings,mapping_section):
    target_service_entities = []
    if mapping_section in mappings:
        section = mappings[mapping_section]
        for mapping in section:
            class_name = mapping.get('class_name')
            check_col = mapping.get('check_col')
            if check_col in operations_script:
                target_service_entities.append(f'"{class_name}"')

    final_entities = "    " + ",\n    ".join(target_service_entities) + "\n"
    final_config = package_config_template.format(operations=final_entities,study=study)
    
    with open("ingest_package_config.py", 'w') as f:
        f.write(final_config)
    with open("transform_module.py", 'w') as f:
        f.write(transform_module_template_sample)

def generate_ingest_config_genomic(study,operations_script,mappings,mapping_section):
    target_service_entities = []
    if mapping_section in mappings:
        section = mappings[mapping_section]
        for mapping in section:
            class_name = mapping.get('class_name')
            check_col = mapping.get('check_col')
            if check_col in operations_script:
                target_service_entities.append(f'"{class_name}"')

    final_entities = "    " + ",\n    ".join(target_service_entities) + "\n"
    final_config = package_config_template.format(operations=final_entities,study=study)
    
    with open("ingest_package_config.py", 'w') as f:
        f.write(final_config)
    with open("transform_module.py", 'w') as f:
        f.write(transform_module_template_genomic)

def generate_ingest_config_both(study,operations_script,mappings,mapping_section):
    target_service_entities = []
    if mapping_section in mappings:
        section = mappings[mapping_section]
        for mapping in section:
            class_name = mapping.get('class_name')
            check_col = mapping.get('check_col')
            if check_col in operations_script:
                target_service_entities.append(f'"{class_name}"')

    final_entities = "    " + ",\n    ".join(target_service_entities) + "\n"
    final_config = package_config_template.format(operations=final_entities,study=study)
    
    with open("ingest_package_config.py", 'w') as f:
        f.write(final_config)
    with open("transform_module.py", 'w') as f:
        f.write(transform_module_template)


if use_case == 'sample_only':
    sample_operations_script = generate_operations_script(sample_df, mappings, "sample_metadata")
    save_script(source_sample_template, sample_operations_script, "config_sample.py")
    total_operations = sample_operations_script
    generate_ingest_config_sample(study_str, total_operations, mappings, "available_ingest_targets")
    
elif use_case == 'genomic_only':
    gf_operations_script = generate_operations_script(gf_df, mappings, "harmonized_genomics_metadata")
    save_script(source_genomics_template, gf_operations_script, "config_genomic_file.py")

    hash_operations_script = generate_operations_script(hash_df, mappings, "hash_metadata")
    save_script(source_hash_template, hash_operations_script, "config_hash.py")

    total_operations = gf_operations_script
    generate_ingest_config_genomic(study_str, total_operations, mappings, "available_ingest_targets")

elif use_case == 'both':
    sample_operations_script = generate_operations_script(sample_df, mappings, "sample_metadata")
    save_script(source_sample_template, sample_operations_script,"config_sample.py")
    gf_operations_script = generate_operations_script(gf_df, mappings, "source_genomics_metadata")
    save_script(source_genomics_template, gf_operations_script, "config_genomic_file.py")
    hash_operations_script = generate_operations_script(hash_df, mappings, "hash_metadata")
    save_script(source_hash_template, hash_operations_script, "config_hash.py")

    total_operations = sample_operations_script + gf_operations_script
    generate_ingest_config_both(study_str, total_operations, mappings, "available_ingest_targets")