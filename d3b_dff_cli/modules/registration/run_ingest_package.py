import argparse
import json
import os
import subprocess
import sys

def main(args):
    package_name = args.INGEST_PACKAGE_PATH

    # Run ingest package
    subprocess.run(["kidsfirst", "ingest", package_name,
                    "--clear_cache",
                    "--no_validate",
                    "--log_level", "debug",
                    "-t", "https://kf-api-dataservice.kidsfirstdrc.org"], check=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the pipeline to ingest the package.")
    parser.add_argument("INGEST_PACKAGE_PATH", help="Path to the data ingest package directory.")
    args = parser.parse_args()
    main(args)
