#!/usr/bin/env python
import os
import sys
import subprocess
import argparse

def check_samtools():
    try:
        subprocess.run(['samtools', '--version'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    except subprocess.CalledProcessError as e:
        print("Error: 'samtools' not found. Please install samtools.")
        sys.exit(1)

def check_rg(file_path):
    try:
        # Check if the BAM header contains an RG line
        header_result = subprocess.run(['samtools', 'view', '-H', file_path], stdout=subprocess.PIPE, check=True)
        header_lines = header_result.stdout.decode().split('\n')
        if not any(line.startswith('@RG') for line in header_lines):
            sample_result = subprocess.run(['samtools', 'view', file_path], stdout=subprocess.PIPE, check=True)
            sample_lines = sample_result.stdout.decode().split('\n')[:5000]
            if not any('RG:' in line for line in sample_lines):
                print(f"Error: No @RG found in the header or the first 5k reads of {file_path}.")
    except subprocess.CalledProcessError as e:
        print(f"Error processing {file_path}: {e}")

def main(args):
    # Check if samtools is available before processing arguments
    check_samtools()
    for bam_file in args.bam_files:
        # Check if bam format
        if not bam_file.endswith((".bam", ".BAM")):
            print(f"Error: Bam file {bam_file} must end with '.bam' or '.BAM'")
            continue
        
        if not os.path.exists(bam_file):
            print(f"File not found: {bam_file}")
        else:
            check_rg(bam_file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check the presence of @RG information in the header or the first 5k reads of BAM files.")
    parser.add_argument("bam_files", nargs="+", help="One or more BAM files to check.")
    args = parser.parse_args()
    main(args)
