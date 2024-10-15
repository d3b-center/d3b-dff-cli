"""Functions to analyze volumes using the Dewrangle GraphQL API"""

import os
import sys
import traceback
import configparser
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from datetime import datetime
from . import helper_functions as hf


def parse_hash_args(args):
    """
    Parse arguments for use in load_and_hash_volume.
    """
    prefix = args.prefix
    region = args.region
    study = args.study
    bucket = args.bucket
    aws_cred = args.credential
    billing = args.billing

    return (prefix, region, study, bucket, aws_cred, billing)


def add_volume(client, study_id, prefix, region, bucket, aws_cred):
    """Run Dewrangle create volume mutation."""

    # prepare mutation

    mutation = gql(
        """
        mutation VolumeCreateMutation($input: VolumeCreateInput!) {
            volumeCreate(input: $input) {
                errors {
                    ... on MutationError {
                        message
                        field
                    }
                }
                volume {
                    name
                    id
                }    
            }
        }
        """
    )

    params = {
        "input": {
            "name": bucket,
            "region": region,
            "studyId": study_id,
            "credentialId": aws_cred,
        }
    }

    if prefix is not None:
        params["input"]["pathPrefix"] = prefix

    # run mutation
    result = client.execute(mutation, variable_values=params)

    hf.check_mutation_result(result)

    volume_id = result["volumeCreate"]["volume"]["id"]

    return volume_id


def list_and_hash_volume(client, volume_id, billing_id):
    """Run Dewrangle list and hash volume mutation."""

    # prepare mutation
    mutation = gql(
        """
        mutation VolumeListHashMutation($id: ID!, $input: VolumeListAndHashInput!) {
            volumeListAndHash(id: $id, input: $input) {
                errors {
                    ... on MutationError {
                        message
                        field
                    }
                }
                job {
                    id
                }    
            }
        }
        """
    )

    params = {"id": volume_id}
    params["input"] = {"billingGroupId": billing_id}

    # run mutation
    result = client.execute(mutation, variable_values=params)

    hf.check_mutation_result(result)

    job_id = result["volumeListAndHash"]["job"]["id"]

    return job_id


def list_volume(client, volume_id):
    """Run Dewrangle list volume mutation."""

    # prepare mutation
    mutation = gql(
        """
        mutation VolumeListMutation($id: ID!) {
            volumeList(id: $id) {
                errors {
                    ... on MutationError {
                        message
                        field
                    }
                }
                job {
                    id
                }    
            }
        }
        """
    )

    params = {"id": volume_id}

    # run mutation
    result = client.execute(mutation, variable_values=params)

    hf.check_mutation_result(result)

    job_id = result["volumeList"]["job"]["id"]

    return job_id


def load_and_run_job(
    bucket_name,
    study_name,
    region,
    job_type,
    prefix=None,
    billing=None,
    aws_cred=None,
    token=None,
):
    """
    Wrapper function that checks if a volume is loaded, and either hashes or lists it.
    Inputs: AWS bucket name, study name, aws region, and optional volume prefix.
    Output: job id of parent job created when volume is hashed.
    """

    client = hf.create_gql_client(api_key=token)

    job_id = None

    try:
        # get study and org ids
        study_id = hf.get_study_id(client, study_name)
        org_id = hf.get_org_id_from_study(client, study_id)

        # check if volume loaded to study
        study_volumes = hf.get_study_volumes(client, study_id)
        volume_id = hf.process_volumes(
            study_id, study_volumes, vname=bucket_name, prefix=prefix
        )

        if volume_id is None:
            # need to load, get credential
            aws_cred_id = hf.get_cred_id(client, study_id, aws_cred)

            # load it
            volume_id = add_volume(
                client, study_id, prefix, region, bucket_name, aws_cred_id
            )

        if job_type == "hash":
            # get billing group id
            billing_group_id = hf.get_billing_id(client, org_id, billing)
            job_id = list_and_hash_volume(client, volume_id, billing_group_id)

        elif job_type == "list":
            job_id = list_volume(client, volume_id)

    except Exception:
        print(
            "The following error occurred trying to hash {}: {}".format(
                bucket_name, traceback.format_exc()
            ),
            file=sys.stderr,
        )

    return job_id


def run_list(args):
    """Other main function to load and list a volume."""
    job_id = load_and_run_job(
        args.bucket,
        args.study,
        args.region,
        "list",
        args.prefix,
        args.billing,
        args.credential,
    )
    print(job_id)


def main(args):
    """Main function. Call load_and_hash and output job_id."""
    job_id = load_and_run_job(
        args.bucket,
        args.study,
        args.region,
        "hash",
        args.prefix,
        args.billing,
        args.credential,
    )
    print(job_id)
