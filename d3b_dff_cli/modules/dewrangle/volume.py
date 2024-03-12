"""Functions to analyze volumes using the Dewrangle GraphQL API"""
import os
import sys
import traceback
import argparse
import configparser
import requests
import pandas as pd
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from datetime import datetime

def parse_hash_args(args):
    """
    Parse arguments for use in load_and_hash_volume.
    """
    # optional args
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-prefix",
        help="Optional, Path prefix. Default: None",
        default=None,
        required=False,
    )
    parser.add_argument(
        "-region",
        help="Optional, Bucket AWS region code. Default: us-east-1",
        default="us-east-1",
        required=False,
    )
    parser.add_argument(
        "-billing",
        help="Optional, billing group name. When not provided, use default billing group for organization",
        default=None,
        required=False,
    )
    parser.add_argument(
        "-credential",
        help="Dewrangle AWS credential name. Default, try to find available credential.",
        required=False,
    )
    # required args
    required_args = parser.add_argument_group("required arguments")
    required_args.add_argument(
        "-study", help="Study name, global id, or study id", required=True
    )
    required_args.add_argument("-bucket", help="Bucket name", required=True)

    # parse and return arguments
    args = parser.parse_args()
    prefix = args.prefix
    region = args.region
    study = args.study
    bucket = args.bucket
    aws_cred = args.credential
    billing = args.billing

    return (prefix, region, study, bucket, aws_cred, billing)


def create_gql_client(endpoint=None, api_key=None):
    """Create GraphQL client connection"""

    # default endpoint
    if endpoint is None:
        endpoint = "https://dewrangle.com/api/graphql"

    if api_key:
        req_header = {"X-Api-Key": api_key}
    else:
        req_header = {"X-Api-Key": get_api_credential()}

    transport = AIOHTTPTransport(
        url=endpoint,
        headers=req_header,
    )
    client = Client(transport=transport, fetch_schema_from_transport=True)

    return client


def get_all_studies(client):
    """Query all available studies, return study ids and names"""

    studies = {}

    # set up query to get all available studies
    query = gql(
        """
        query {
            viewer {
                organizationUsers {
                    edges {
                        node {
                            organization {
                                name
                                id
                                studies {
                                    edges {
                                        node {
                                            name
                                            id
                                            globalId
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        """
    )

    # run query
    result = client.execute(query)

    for org_edge in result["viewer"]["organizationUsers"]["edges"]:
        for study_edge in org_edge["node"]["organization"]["studies"]["edges"]:
            study = study_edge["node"]
            id = study["id"]
            name = study["name"]
            global_id = study["globalId"]
            studies[id] = {"name": name, "global_id": global_id}

    return studies


def get_study_id(client, study_name):
    """Query all available studies, return study id"""

    study_id = ""
    study_ids = []

    # get a dictionary of all study ids and names
    studies = get_all_studies(client)

    # loop through query results, find the study we're looking for and it's volumes
    for study in studies:
        if study_name in [study, studies[study]["global_id"], studies[study]["name"]]:
            print(studies[study]["global_id"])
            study_ids.append(study)

    if len(study_ids) == 1:
        study_id = study_ids[0]
    elif len(study_ids) == 0:
        raise ValueError("Study {} not found".format(study_name))
    else:
        raise ValueError(
            "Study {} found multiple times. Please delete or rename studies so there is only one {}".format(
                study_name, study_name
            )
        )

    return study_id


def get_org_id_from_study(client, study_id):
    """Query study id and get the id of the organization it's in"""

    org_id = ""
    # set up query to get all available studies
    query = gql(
        """
        query Study_Query($id: ID!) {
            study: node(id: $id) {
                ... on Study {
                    organization {
                        id
                    }
                }
            }
        }
        """
    )

    params = {"id": study_id}

    # run query
    result = client.execute(query, params)

    org_id = result["study"]["organization"]["id"]

    return org_id


def load_and_hash_volume(args):
    """
    Wrapper function that checks if a volume is loaded, and hashes it.
    Inputs: AWS bucket (volume) name, study name, aws region, and optional volume prefix.
    Output: job id of parent job created when volume is hashed.
    """
    print("Coming soon load and hash volume")

    prefix, region, study_name, volume_name, aws_cred, billing = parse_hash_args(args)

    if client is None:
        client = create_gql_client()

    job_id = None

    try:
        # get study and org ids
        study_id = get_study_id(client, study_name)
        org_id = get_org_id_from_study(client, study_id)

        # get billing group id
        billing_group_id = get_billing_id(client, org_id, billing)

        # check if volume loaded to study
        study_volumes = get_study_volumes(client, study_id)
        volume_id = process_volumes(study_id, study_volumes, vname=volume_name)

        if volume_id is None:
            # ineed to load, get credential
            aws_cred_id = get_cred_id(client, study_id, aws_cred)

            # load it
            volume_id = add_volume(
                client, study_id, prefix, region, volume_name, aws_cred_id
            )

        # hash
        job_id = list_and_hash_volume(client, volume_id, billing_group_id)

    except Exception:
        print(
            "The following error occurred trying to hash {}: {}".format(
                volume_name, traceback.format_exc()
            ),
            file=sys.stderr,
        )

    return job_id
