"""Functions download job results using the Dewrangle GraphQL API"""

import os
import sys
import traceback
import configparser
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from datetime import datetime
from . import helper_functions as hf


def parse_job_args(args):
    """
    Parse arguments for use in load_and_hash_volume.
    """
    prefix = args.prefix
    region = args.region
    study = args.study
    bucket = args.bucket
    aws_cred = args.credential
    billing = args.billing
    job = args.job

    return (prefix, region, study, bucket, aws_cred, billing, job)


def load_and_hash_volume(
    bucket_name,
    study_name,
    region,
    prefix=None,
    billing=None,
    aws_cred=None,
    token=None,
):
    """
    Wrapper function that checks if a volume is loaded, and hashes it.
    Inputs: AWS bucket name, study name, aws region, and optional volume prefix.
    Output: job id of parent job created when volume is hashed.
    """

    client = hf.create_gql_client(api_key=token)

    job_id = None

    try:
        # get study and org ids
        study_id = hf.get_study_id(client, study_name)
        org_id = hf.get_org_id_from_study(client, study_id)

        # get billing group id
        billing_group_id = hf.get_billing_id(client, org_id, billing)

        # check if volume loaded to study
        study_volumes = hf.get_study_volumes(client, study_id)
        volume_id = hf.process_volumes(
            study_id, study_volumes, vname=bucket_name, prefix=prefix
        )

        print(volume_id)
        exit(1)


        if volume_id is None:
            # need to load, get credential
            aws_cred_id = hf.get_cred_id(client, study_id, aws_cred)

            # load it
            volume_id = add_volume(
                client, study_id, prefix, region, bucket_name, aws_cred_id
            )

        # hash
        job_id = list_and_hash_volume(client, volume_id, billing_group_id)

    except Exception:
        print(
            "The following error occurred trying to hash {}: {}".format(
                bucket_name, traceback.format_exc()
            ),
            file=sys.stderr,
        )

    return job_id


def main(args):
    """Main function."""

    # need to have 2 functions: one to get job results and one to find jobs

    job_id = load_and_hash_volume(
        args.bucket,
        args.study,
        args.region,
        args.prefix,
        args.billing,
        args.credential,
    )
    print(job_id)
