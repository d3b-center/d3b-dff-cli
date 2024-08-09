# General Registration Process

This process is designed to register source sample and genomic files. It requires the sample manifest, genomic manifest, and hash manifest. The accepted column names for each manifest are defined in the `templates/accepted_manifest_fields.csv`. 

## Files and Structure
1. `inputs_manifest.json`
   
    This JSON file defines the paths to the required manifest files for the ingestion process.
    
    `TODO` - Replace the internal file path with your own registration manifest path.

2. `ingest_mapping.json`
    
        This JSON file specifies the mapping rules for the provided manifest columns to the DataService concept. It includes:
        - **ingest_all_targets**: All DataService supported ingest entities.
        - **available_ingest_targets**: Ingest entities currently supported by our process.
        - **sample_metadata**, **source_genomics_metadata**, **harmonized_genomics_metadata** and **hash_metadata**: Define the mapping rules. 
   
3. `templates` Folder
   - `accepted_manifest_fields.csv`
  
        This CSV file defines the accepted column names for each manifest. The sample and genomic manifest column names are based on the [kf-lib-data-ingest](https://github.com/kids-first/kf-lib-data-ingest/blob/master/kf_lib_data_ingest/common/concept_schema.py), while the hash manifest header is derived from the dewrangle report and includes the `etag` field.

   - `sampleManifest_template.csv`: Template for the sample manifest.
   - `source_genomics_template.csv`: Template for the source genomics manifest.
   - `harmonized_genomics_template.csv`: Template for the harmonized genomics manifest.
   - `hashReport_template.csv`: Template for the genomics hash report.
  
    Your manifest does not need to contain all columns from the templates. However, the column names should be consistent with those in the templates to ensure proper processing. 

    <details>
    <summary><strong>Minimum Required Columns</strong></summary>

    ```bash
    sample_manifest: [
        "study_id", # will become CONCEPT.PROJECT.ID.
        "subject_id", # will become CONCEPT.PARTICIPANT.ID.
        "sample_id", # will become CONCEPT.BIOSPECIMEN_GROUP.ID.
        "aliquot_id", # will become CONCEPT.BIOSPECIMEN.ID.
        "tissue_type", # will become CONCEPT.BIOSPECIMEN.TISSUE_TYPE.
        "analyte_type", # will become CONCEPT.BIOSPECIMEN.ANALYTE.
        "visible", # required for every target entities.
        "visibility_reason", # required for every target entities.
        "kf_sequence_center_id", # Optional, but required when registering sample only
        "family_id", # Optional, but required when registering family relationship
        "family_relationship", # Optional, but required when registering family relationship
        "gender", # Optional, but required when registering family relationship
        "is_proband" # Optional, but required when registering family relationship
    ],
    "source_genomics_manifest": [
        "aliquot_id", # used to connect with sample_manifest.
        "kf_sequence_center_id", # will become CONCEPT.SEQUENCING.CENTER.TARGET_SERVICE_ID.
        "sequencing_id", # will become CONCEPT.SEQUENCING.ID.
        "experiment_strategy", # will become CONCEPT.SEQUENCING.STRATEGY.
        "file_path", # will become CONCEPT.GENOMIC_FILE.ID and CONCEPT.GENOMIC_FILE.URL_LIST.
        "visible",  # required for every target entities.
        "visibility_reason"  # required for every target entities.
    ],
    "harmonized_genomics_manifest": [
        "study_id", # will become CONCEPT.PROJECT.ID.
        "kf_sequencing_experiment_id", # will become CONCEPT.SEQUENCING.TARGET_SERVICE_ID.
        "biospecimen_id", # will become CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID.
        "file_path", # will become CONCEPT.GENOMIC_FILE.ID and CONCEPT.GENOMIC_FILE.URL_LIST.
        "visible",  # required for every target entities.
        "visibility_reason"  # required for every target entities.
    ],
    "hash_manifest": [
        "uri", # will become CONCEPT.GENOMIC_FILE.ID, used to connect with genomics_manifest.
        "sizeInBytes" # will become CONCEPT.GENOMIC_FILE.SIZE.
        # And at least one hash type: etag, md5, sha1, sha256, sha512
    ]
    ```

## Run ingest package
Default `mapping_manifest` file: `ingest_mapping.json`.

1. Ingest source genomics file
    - Inputs manifests:  `sample_manifest` + `source_genomics_manifest` + `hash_manifest`
    - Example `inputs_manifest.json`

        ```JSON
        {
            "package_name": "test_ingest",
            "sample_manifest": "templates/sampleManifest_template.csv",
            "genomics_manifest": "templates/source_genomics_template.csv",
            "hash_manifest": "templates/hashReport_template.csv",
            "mapping_manifest": ""
        }
        ```

2. Ingest sample file
    - Inputs manifests:  `sample_manifest`
    - Example `inputs_manifest.json`

        ```JSON
        {
            "package_name": "test_ingest_sample",
            "sample_manifest": "templates/sampleManifest_template.csv",
            "genomics_manifest": "",
            "hash_manifest": "",
            "mapping_manifest": ""
        }
        ```

3. Ingest harmonized genomics file
    - Inputs manifests:  `harmonized_genomics_manifest` + `hash_manifest`
    - Example `inputs_manifest.json`

        ```JSON
        {
            "package_name": "test_ingest_harmonized",
            "sample_manifest": "",
            "genomics_manifest": "templates/harmonized_genomics_template.csv",
            "hash_manifest": "templates/hashReport_template.csv",
            "mapping_manifest": ""
        }
        ```

## Main script
1. `main_prepare_script.py`
   
   This Python script processes the data using the provided manifests. It handles the arguments passed to it and performs the necessary operations to prepare the ingest package.

2. `kidsfirst` cli

    This is defined in [kf-lib-data-ingest](https://github.com/kids-first/kf-lib-data-ingest/tree/master).

