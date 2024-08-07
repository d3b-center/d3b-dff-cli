import psycopg2
import pandas as pd
import os
import configparser
import argparse
from datetime import datetime

def get_db_credentials(credentials_path):
    """Extract database credentials from a configuration file."""
    if not os.path.exists(credentials_path):
        raise SystemExit(f"Error: Credentials file not found: {credentials_path}")

    config = configparser.ConfigParser()
    config.read(credentials_path)
    try:
        username = config.get('kfpostgresprd', 'username')
        password = config.get('kfpostgresprd', 'password')
        hostname = config.get('kfpostgresprd', 'hostname')
        dbname = config.get('kfpostgresprd', 'dbname')
    except configparser.NoSectionError as e:
        raise SystemExit(f"Error: Missing 'kfpostgresprd' section or required keys in credentials file.\n{e}")

    return username, password, hostname, dbname

def main(args):
    """Main function to perform QC checks after registration."""
    credentials_path = os.path.expanduser('~/.d3bcli/credentials')
    
    # Create KF database conncection
    username, password, hostname, dbname = get_db_credentials(credentials_path)
    conn = psycopg2.connect(dbname=dbname, user=username, password=password, host=hostname)

    date = args.date
    date_formatted = datetime.strptime(date, '%Y-%m-%d').strftime('%Y%m%d')

    # STEP1: SQL query to get registered fields
    registered_fileds = f"""
        SELECT DISTINCT
            pt.kf_id AS participant_id,
            pt.family_id,
            bsgf.biospecimen_id,
            segf.sequencing_experiment_id,
            gf.kf_id AS genomic_file_id
        FROM participant AS pt
        LEFT JOIN biospecimen AS bs ON pt.kf_id = bs.participant_id
        LEFT JOIN biospecimen_genomic_file AS bsgf ON bs.kf_id = bsgf.biospecimen_id
        LEFT JOIN genomic_file AS gf ON bsgf.genomic_file_id = gf.kf_id
        LEFT JOIN sequencing_experiment_genomic_file AS segf ON gf.kf_id = segf.genomic_file_id
        WHERE DATE(pt.created_at) = '{date}'
            OR DATE(bs.created_at) = '{date}'
            OR DATE(bsgf.created_at) = '{date}'
            OR DATE(segf.created_at) = '{date}'
            OR DATE(gf.created_at) = '{date}'
    """
    df_registered = pd.read_sql(registered_fileds, conn)
    if df_registered.empty:
        print(f"No registered data on {date}")
    else:
        df_registered.to_csv(f"{date_formatted}.registered_data.csv", index=False)
        print(f"Saved all registered data on {date}")

    # STEP2: Count linked genomic files
    count_gf = f"""
        SELECT COUNT(DISTINCT gf.kf_id) AS count
        FROM genomic_file gf
        LEFT JOIN sequencing_experiment_genomic_file segf ON gf.kf_id = segf.genomic_file_id
        LEFT JOIN biospecimen_genomic_file bsgf ON gf.kf_id = bsgf.genomic_file_id
        WHERE DATE(gf.created_at) = '{date}'
            AND segf.genomic_file_id IS NOT NULL
            AND bsgf.genomic_file_id IS NOT NULL
    """
    df_count_gf = pd.read_sql(count_gf, conn)
    count = df_count_gf['count'][0]

    if count != 0:
        linked_gf = f"""
            SELECT DISTINCT
                gf.kf_id AS genomic_file_id, 
                bsgf.biospecimen_id, 
                segf.sequencing_experiment_id, 
                gf.external_id
            FROM genomic_file gf
            LEFT JOIN sequencing_experiment_genomic_file segf ON gf.kf_id = segf.genomic_file_id
            LEFT JOIN biospecimen_genomic_file bsgf ON gf.kf_id = bsgf.genomic_file_id
            WHERE DATE(gf.created_at) = '{date}'
            AND segf.genomic_file_id IS NOT NULL
            AND bsgf.genomic_file_id IS NOT NULL
        """
        df_linked_gf = pd.read_sql(linked_gf, conn)
        df_linked_gf.to_csv(f"{date_formatted}.linked_genomic_files.csv", index=False)
        print(f"Linked genomic file COUNT: {count}")
    else:
        unlinked = f"""
            SELECT DISTINCT
                gf.kf_id AS genomic_file_id,
                bsgf.biospecimen_id,
                gf.external_id
            FROM genomic_file gf
            LEFT JOIN sequencing_experiment_genomic_file segf ON gf.kf_id = segf.genomic_file_id
            LEFT JOIN biospecimen_genomic_file bsgf ON gf.kf_id = bsgf.genomic_file_id
            WHERE DATE(gf.created_at) = '{date}'
            AND (segf.genomic_file_id IS NULL OR bsgf.genomic_file_id IS NULL)
        """
        df_unlinked = pd.read_sql(unlinked, conn)
        if not df_unlinked.empty:
            df_unlinked.to_csv(f"{date_formatted}.unlinked_genomic_files.csv", index=False)
            print("Warning: There are unlinked(bs_id/sequencing_experiment_id) genomic files")
    

    # STEP3: Check linked duplication genomic files
    linked_duplicates = f"""
        SELECT COUNT(*) AS count, bsgf.biospecimen_id, gf.external_id
        FROM genomic_file gf
        LEFT JOIN biospecimen_genomic_file bsgf ON gf.kf_id = bsgf.genomic_file_id
        WHERE DATE(gf.created_at) = '{date}'
            AND bsgf.genomic_file_id IS NOT NULL
        GROUP BY bsgf.biospecimen_id, gf.external_id
        HAVING COUNT(*) > 1
    """

    df_linked_duplicates = pd.read_sql(linked_duplicates, conn)
    if not df_linked_duplicates.empty:
        df_linked_duplicates.to_csv(f"{date_formatted}.linked_duplications.csv", index=False)
        print("Warning: There are duplications when grouping by bs_id, gf.external_id")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="QC step after registration.")
    parser.add_argument('-date', required=True, help="Registration date, YYYY-MM-DD (e.g., 2024-7-20).")
    args = parser.parse_args()
    main(args)