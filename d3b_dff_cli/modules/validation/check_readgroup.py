#!/usr/bin/env python
import os
import argparse
import pysam

def check_rg(file_path):
    """Check if the BAM header contains an RG line."""
    try:
        bam = pysam.AlignmentFile(file_path, "rb")
        header = bam.header.to_dict()
        if not "RG" in header:
            # search first 5k lines, and see if any start with @RG
            sample_lines = bam.head(5000)
            if not any("RG:" in line.to_string() for line in sample_lines):
                print(
                    f"Error: No @RG found in the header or the first 5k reads of {file_path}."
                )
    except Exception as e:
        print(f"Error processing {file_path}: {e}")


def main(args):
    for bam_file in args.bam_files:
        # Check if bam format
        if not bam_file.endswith((".bam", ".BAM")):
            print(f"Error: Bam file {bam_file} must end with '.bam' or '.BAM'")
            continue

        if not os.path.exists(bam_file):
            print(f"File not found: {bam_file}")
        else:
            #deprecated_check_rg(bam_file)
            check_rg(bam_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Check the presence of @RG information in the header or the first 5k reads of BAM files."
    )
    parser.add_argument("bam_files", nargs="+", help="One or more BAM files to check.")
    args = parser.parse_args()
    main(args)
