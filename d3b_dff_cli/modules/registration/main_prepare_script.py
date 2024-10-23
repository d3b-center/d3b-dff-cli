import pandas as pd
import json
import black
from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-sample", help="sample metadata manifest(.csv)", required=False)
parser.add_argument("-genomic", help="genomics file metadata manifest(.csv)", required=False)
parser.add_argument("-hash", help="hash report manifest(.csv)", required=False)
parser.add_argument("-mapping", help="ingest mapping file(.json)", required=False)
args = parser.parse_args()


def load_data(file):
    # Load data based on file extension
    file_extension = file.split('.')[-1].lower()
    if file_extension == 'csv':
        manifest_data = pd.read_csv(file)
    elif file_extension == 'tsv':
        manifest_data = pd.read_csv(file, delimiter='\t')
    else:
        raise ValueError("Unsupported file format. Please provide a CSV or TSV file.")
    
    # Convert manifest to lowercase
    manifest_data = manifest_data.apply(lambda col: col.astype(str).str.lower() if col.dtype.name in ['object', 'bool'] else col)

    return manifest_data

# Initialize DataFrames
sample_df = gf_df = hash_df = None

# Load input files
if args.sample and not args.genomic:
    sample_df = load_data(args.sample)
    use_case = 'sample_only'
elif args.genomic and not args.sample:
    gf_df = load_data(args.genomic)
    hash_df = load_data(args.hash)
    use_case = 'genomic_only'
elif args.sample and args.genomic:
    sample_df = load_data(args.sample)
    gf_df = load_data(args.genomic)
    hash_df = load_data(args.hash)
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
        study_str = str(study_values[0].upper())
        return study_str
    else:
        print(f"ERROR: study_id is {study_values}")
        sys.exit()

######## Check minimum required fields
# Required columns
required_fields = {
    'sample_manifest': ["study_id","subject_id","sample_id","aliquot_id","tissue_type","analyte_type","visible","visibility_reason"],
    'genomics_manifest': ["aliquot_id","kf_sequence_center_id","sequencing_id","experiment_strategy","file_path","visible","visibility_reason"],
    'harmonized_manifest': ["biospecimen_id","file_path","sequencing_experiment_id","visible","visibility_reason"],
    'hash_manifest': ["uri","sizeInBytes"],
    'sample_only_manifest': ["study_id","kf_sequence_center_id","subject_id","sample_id","aliquot_id","analyte_type","visible","visibility_reason"],
    "family_relationship":["family_id", "family_relationship", "is_proband"]
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
    missing_columns |= check_required_columns(sample_df, 'sample_only_manifest')
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
    sys.exit(1)

## Define templates
source_sample_template = """
import os
import re
import pandas as pd
from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.constants import COMMON, GENOMIC_FILE
from kf_lib_data_ingest.common.pandas_utils import Split
from kf_lib_data_ingest.common.misc import import_module_from_file, str_to_obj
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

source_sample_family_relationship_template = """
import os
import re
import pandas as pd
from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.constants import COMMON, GENOMIC_FILE
from kf_lib_data_ingest.common.misc import import_module_from_file, str_to_obj
from kf_lib_data_ingest.common.pandas_utils import Split
from kf_lib_data_ingest.etl.extract.operations import (
    constant_map,
    keep_map,
    row_map,
    value_map,
)

source_data_url = "file://../data/sample_manifest.csv"

def do_after_read(df):
    df["is_proband"] = df["is_proband"].apply(str_to_obj)
    grouped_df = df.groupby("family_id")
    fdf = grouped_df.apply(build_fr)
    return fdf.reset_index(drop=True)

def determine_relationship(child_g):
    return constants.RELATIONSHIP.DAUGHTER if child_g.lower() == "female" else constants.RELATIONSHIP.SON if child_g.lower() == "male" else constants.RELATIONSHIP.CHILD

def build_family_relationships(fam_id, child_id, child_g, family_df):
    relations = []
    child_r = determine_relationship(child_g)

    def add_relation(person1_id, person1_g, person2_id, person2_g, relation):
        relations.append({{
            CONCEPT.FAMILY_RELATIONSHIP.PERSON1.ID: person1_id,
            CONCEPT.FAMILY_RELATIONSHIP.PERSON2.ID: person2_id,
            CONCEPT.FAMILY_RELATIONSHIP.RELATION_FROM_1_TO_2: relation,
            CONCEPT.FAMILY_RELATIONSHIP.PERSON1.GENDER: person1_g,
            CONCEPT.FAMILY_RELATIONSHIP.PERSON2.GENDER: person2_g
        }})

    if "father" in family_df["family_relationship"].str.lower().values:
        father_id = family_df[family_df["family_relationship"].str.lower() == "father"]["subject_id"].values[0]
        father_g = family_df[family_df["subject_id"] == father_id]["gender"].values[0]
        add_relation(child_id, child_g, father_id, father_g, child_r)
        add_relation(father_id, father_g, child_id, child_g, constants.RELATIONSHIP.FATHER)
    
    if "mother" in family_df["family_relationship"].str.lower().values:
        mother_id = family_df[family_df["family_relationship"].str.lower() == "mother"]["subject_id"].values[0]
        mother_g = family_df[family_df["subject_id"] == mother_id]["gender"].values[0]
        add_relation(child_id, child_g, mother_id, mother_g, child_r)
        add_relation(mother_id, mother_g, child_id, child_g, constants.RELATIONSHIP.MOTHER)

    for sibling_id, sibling_g in zip(
            family_df[family_df["family_relationship"].str.lower() == "sibling"]["subject_id"].values,
            family_df[family_df["family_relationship"].str.lower() == "sibling"]["gender"].values):
        sibling_r = determine_relationship(sibling_g)
        add_relation(child_id, child_g, sibling_id, sibling_g, sibling_r)
        add_relation(sibling_id, sibling_g, child_id, child_g, constants.RELATIONSHIP.SIBLING)

    relationship_df = pd.DataFrame(relations)
    relationship_df[CONCEPT.FAMILY.ID] = fam_id
    merged_df = pd.merge(family_df, relationship_df, left_on="subject_id", right_on=CONCEPT.FAMILY_RELATIONSHIP.PERSON1.ID, how="left")
    
    return merged_df

def build_fr(family_df):
    fam_id = family_df["family_id"].iloc[0]
    if len(family_df) == 1 or family_df["is_proband"].sum() != 1:
        return family_df.assign(**{{
            CONCEPT.FAMILY_RELATIONSHIP.PERSON1.ID: None,
            CONCEPT.FAMILY_RELATIONSHIP.PERSON2.ID: None,
            CONCEPT.FAMILY_RELATIONSHIP.RELATION_FROM_1_TO_2: None,
            CONCEPT.FAMILY_RELATIONSHIP.PERSON1.GENDER: None
        }})

    child_id = family_df[family_df["is_proband"] == True]["subject_id"].values[0]
    child_g = family_df[family_df["subject_id"] == child_id]["gender"].values[0]
    
    out_df = build_family_relationships(fam_id, child_id, child_g, family_df)
    return out_df

operations = [
    keep_map(in_col=CONCEPT.FAMILY_RELATIONSHIP.PERSON1.ID, out_col=CONCEPT.FAMILY_RELATIONSHIP.PERSON1.ID),
    keep_map(in_col=CONCEPT.FAMILY_RELATIONSHIP.PERSON2.ID,out_col=CONCEPT.FAMILY_RELATIONSHIP.PERSON2.ID),
    keep_map(in_col=CONCEPT.FAMILY_RELATIONSHIP.RELATION_FROM_1_TO_2, out_col=CONCEPT.FAMILY_RELATIONSHIP.RELATION_FROM_1_TO_2),
    keep_map(in_col=CONCEPT.FAMILY_RELATIONSHIP.PERSON1.GENDER, out_col=CONCEPT.FAMILY_RELATIONSHIP.PERSON1.GENDER),
    keep_map(in_col=CONCEPT.FAMILY_RELATIONSHIP.PERSON2.GENDER, out_col=CONCEPT.FAMILY_RELATIONSHIP.PERSON2.GENDER),
    constant_map(m="Other", out_col=CONCEPT.FAMILY_RELATIONSHIP.VISIBILTIY_REASON),
    constant_map(m="Other", out_col=CONCEPT.FAMILY_RELATIONSHIP.VISIBILITY_COMMENT),

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

    if 'family_relationship' not in sample_df.columns or sample_df['family_relationship'].isnull().any():
        save_script(source_sample_template, sample_operations_script, "config_sample.py")
    else:
        missing_fr = check_required_columns(sample_df, 'family_relationship')
        if missing_fr:
            sys.exit(1)
        save_script(source_sample_family_relationship_template, sample_operations_script, "config_sample.py")

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
    if 'family_relationship' not in sample_df.columns or sample_df['family_relationship'].isnull().any():
        save_script(source_sample_template, sample_operations_script, "config_sample.py")
    else:
        missing_fr = check_required_columns(sample_df, 'family_relationship')
        if missing_fr:
            sys.exit(1)
        save_script(source_sample_family_relationship_template, sample_operations_script, "config_sample.py")
    gf_operations_script = generate_operations_script(gf_df, mappings, "source_genomics_metadata")
    save_script(source_genomics_template, gf_operations_script, "config_genomic_file.py")
    hash_operations_script = generate_operations_script(hash_df, mappings, "hash_metadata")
    save_script(source_hash_template, hash_operations_script, "config_hash.py")

    total_operations = sample_operations_script + gf_operations_script
    generate_ingest_config_both(study_str, total_operations, mappings, "available_ingest_targets")