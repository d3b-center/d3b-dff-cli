import argparse
import json
import os
import shutil
import subprocess
import sys
import glob

def main(args):
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Load JSON file
    json_file = args.input
    with open(json_file, 'r') as file:
        data = json.load(file)

    # Extract paths from JSON file
    package_name = data.get('package_name')
    genomic_manifest = data.get('genomics_manifest')
    sample_manifest = data.get('sample_manifest')
    hash_manifest = data.get('hash_manifest')
    mapping_json = data.get('mapping_manifest')

    # Set default value for mapping_json if it's empty
    if not mapping_json or mapping_json == "null":
        mapping_json = os.path.join(script_dir, "ingest_mapping.json")

    # Check for missing required fields
    if not package_name:
        print(f"Error: Missing package_name in the {json_file}.")
        sys.exit(1)

    if not sample_manifest and not genomic_manifest and not hash_manifest:
        print(f"Error: At least one file manifest must be provided in the {json_file}.")
        sys.exit(1)

    # Determine use case based on provided manifests
    if sample_manifest and not genomic_manifest and not hash_manifest:
        use_case = "sample_only"
    elif not sample_manifest and genomic_manifest and hash_manifest:
        use_case = "genomic_only"
    elif sample_manifest and genomic_manifest and hash_manifest:
        use_case = "both"
    else:
        print("Error: Incomplete set of manifests provided.")
        sys.exit(1)

    # Step 1: prepare ingest config script
    main_script = os.path.join(script_dir, "main_prepare_script.py")
    prepare_script_args = ["python3", main_script, "-mapping", mapping_json]
    if use_case == "both":
        prepare_script_args += ["-sample", sample_manifest, "-genomic", genomic_manifest, "-hash", hash_manifest]
    elif use_case == "sample_only":
        prepare_script_args += ["-sample", sample_manifest]
    elif use_case == "genomic_only":
        prepare_script_args += ["-genomic", genomic_manifest, "-hash", hash_manifest]

    subprocess.run(prepare_script_args, check=True)

    # Step 2: Create a new package
    subprocess.run(["kidsfirst", "new", "--dest_dir", package_name], check=True)

    # Step 3: Modify the default values in the new package
    patterns_to_remove = [f"{package_name}/data/*.*sv", f"{package_name}/extract_configs/*.py"]
    for pattern in patterns_to_remove:
        files_to_remove = glob.glob(pattern)
        for file in files_to_remove:
            os.remove(file)

    # Copy the manifests to the package directory
    def copy_file(src, dst):
        try:
            shutil.copy2(src, dst)  # Copy the file and overwrite if it exists
        except Exception as e:
            print(f"Error occurred while copying {src}: {e}")

    def copy_and_remove(src, dst):
        try:
            shutil.copy2(src, dst)
            os.remove(src)  # Remove the original file
        except Exception as e:
            print(f"Error occurred while copying {src}: {e}")

    if sample_manifest:
        copy_file(sample_manifest, f"{package_name}/data/sample_manifest.csv")
        copy_and_remove("config_sample.py", f"{package_name}/extract_configs/config_sample.py")

    if genomic_manifest:
        copy_file(genomic_manifest, f"{package_name}/data/genomics_file_manifest.csv")
        copy_and_remove("config_genomic_file.py", f"{package_name}/extract_configs/config_genomic_file.py")

    if hash_manifest:
        copy_file(hash_manifest, f"{package_name}/data/hash_manifest.csv")
        copy_and_remove("config_hash.py", f"{package_name}/extract_configs/config_hash.py")

    copy_and_remove("ingest_package_config.py", f"{package_name}/ingest_package_config.py")
    copy_and_remove("transform_module.py", f"{package_name}/transform_module.py")

    # Step 4: Run ingest package
    print("Start run ingest package")
    subprocess.run(["kidsfirst", "ingest", package_name,
                    "--clear_cache",
                    "--no_validate",
                    "--log_level", "debug",
                    "-t", "https://kf-api-dataservice.kidsfirstdrc.org"], check=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the ingest package.")
    parser.add_argument('--input', required=True, help="Path to the JSON file for registration.")
    args = parser.parse_args()
    main(args)