"""Functions to analyze volumes using the Dewrangle GraphQL API"""

import os
import sys
import traceback
import configparser
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from datetime import datetime


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


def check_mutation_result(result):
    """Check the result of a mutation and handle error(s)"""

    for my_key in result:
        my_error = result[my_key]["errors"]
        if my_error is not None:
            raise RuntimeError(
                "The following error occurred when running mutation:\n{}".format(
                    my_error
                )
            )

    return


def get_api_credential():
    """Get api token from credential file."""
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.expanduser("~"), ".dewrangle", "credentials"))
    return config["default"]["api_key"]


def get_cred_id(client, study_id, cred_name=None):
    """Get credential id"""

    cred_id = None

    # get all credentials in study
    credentials = get_study_credentials(client, study_id)

    cred_id = pick_external_id(cred_name, credentials, "credential")

    return cred_id


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


def get_study_credentials(client, study_id):
    """Get credential ids from a study."""

    # query all studies and credentials the user has access to.
    # in the future, this should be a simpler query to get study id from study name
    credentials = {}

    # set up query to get all credentials in the study
    query = gql(
        """
        query Study_Query($id: ID!) {
            study: node(id: $id) {
                id
                ... on Study {
                    credentials {
                        edges {
                            node {
                                id
                                name
                                key
                            }
                        }
                    }
                }
            }
        }
        """
    )

    params = {"id": study_id}

    # run query
    result = client.execute(query, variable_values=params)

    # loop through query results, find the study we're looking for and it's volumes
    for study in result:
        for cred_edge in result[study]["credentials"]["edges"]:
            cred = cred_edge["node"]
            cid = cred["id"]
            name = cred["name"]
            key = cred["key"]
            credentials[cid] = {"name": name, "key": key}

    return credentials


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


def get_billing_id(client, org_id, billing=None):
    "Get billing group id. If a name is provided, check it exists. If not return org default."

    # first get a list of organizations and billing groups
    billing_groups = get_billing_groups(client, org_id)

    billing_id = pick_external_id(billing, billing_groups, "billing_group")

    return billing_id


def get_study_volumes(client, study_id):
    """Query study id, and return volumes in that study"""
    study_volumes = {}
    # set up query to get all available studies
    query = gql(
        """
        query Study_Query($id: ID!) {
            study: node(id: $id) {
                ... on Study {
                    volumes {
                        edges {
                            node {
                                id
                                name
                            }
                        }
                    }
                }
            }
        }
        """
    )

    params = {"id": study_id}

    # run query
    result = client.execute(query, params)

    for study in result:
        for volume_edge in result["study"]["volumes"]["edges"]:
            volume = volume_edge["node"]
            vid = volume["id"]
            vname = volume["name"]
            study_volumes[vid] = vname

    return study_volumes


def process_volumes(study, volumes, **kwargs):
    """Check if a volume is already loaded to a study.
    Inputs: study id, dictionary of volumes in the study, optionally volume name or volume id.
    Outputs: volume id"""
    volume_id = kwargs.get("vid", None)
    vname = kwargs.get("vname", None)

    if volume_id:
        if volume_id not in volumes.keys():
            raise ValueError(
                "Volume id not present in study. Ensure you are providing the whole volume id."
            )
    else:
        # see how many times the volume was added to the study
        matching_volumes = []
        for vol in volumes:
            if volumes[vol] == vname:
                matching_volumes.append(vol)
        count = len(matching_volumes)

        if count == 0:
            print("{} volume not found in {}".format(vname, study))
        elif count == 1:
            volume_id = matching_volumes[0]
        else:
            print(
                "=============================================================================================="
            )
            print("Multiple volumes named {} found in {}".format(vname, study))
            print(
                "Rerun this script using the '--vid' option with the volume id of the volume you want to delete"
            )
            print("Matching volumes and ids are:")
            for mvol in matching_volumes:
                print("{}: {}".format(vname, mvol))
            print(
                "=============================================================================================="
            )

    return volume_id


def get_volume_jobs(client, vid):
    """Query volume for a list of jobs"""
    jobs = {}

    query = gql(
        """
        query Volume_Job_Query($id: ID!) {
            volume: node(id: $id) {
                id
                ... on Volume {
                    jobs {
                        edges {
                            node {
                                id
                                operation
                                completedAt
                                createdAt
                            }
                        }
                    }
                }
            }
        }
        """
    )

    params = {"id": vid}

    # run query
    result = client.execute(query, variable_values=params)

    # format result
    for vol in result:
        for job in result[vol]["jobs"]:
            for node in result[vol]["jobs"][job]:
                id = node["node"]["id"]
                # convert createdAt from string to datetime object
                created = datetime.strptime(
                    node["node"]["createdAt"], "%Y-%m-%dT%H:%M:%S.%fZ"
                )
                op = node["node"]["operation"]
                comp = datetime.strptime(
                    node["node"]["completedAt"], "%Y-%m-%dT%H:%M:%S.%fZ"
                )
                jobs[id] = {"operation": op, "createdAt": created, "completedAt": comp}

    return jobs


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

    check_mutation_result(result)

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

    check_mutation_result(result)

    job_id = result["volumeListAndHash"]["job"]["id"]

    return job_id


def pick_external_id(name, externals, external_type):
    """From a dictionary of either credential or billing group ids, pick the one to use."""

    ext_id = None

    org = "other"
    if external_type.lower() == "billing_group":
        org = "organization"
    elif external_type.lower() == "credential":
        org = "study"

    message = ""

    if len(externals) == 1 and name is None:
        ((ext_id, info),) = externals.items()
        ext_id = list(externals.keys())[0]
    elif name:
        for ext in externals:
            if name == externals[ext]["name"]:
                ext_id = ext
        if ext_id is None:
            message = "{} {} not found in {}".format(
                external_type.capitalize(), name, org
            )
    elif len(externals) == 0:
        message = "No credentials in study."
    else:
        message = "Multiple {} found in {} but none provided. Please run again and provide one of the following crdentials ids:{}{}".format(
            external_type, org, "\n", externals
        )

    if ext_id is None:
        raise ValueError(message)

    return ext_id


def get_billing_groups(client, org_id):
    """Get available billing groups for an organization."""

    billing_groups = {}

    # query all organizations, studies, and billing groups the user has access to.
    # set up query to get all available studies
    query = gql(
        """
        query Org_Query($id: ID!) {
            organization: node(id: $id) {
                ... on Organization {
                    billingGroups {
                        edges {
                            node {
                                name
                                id
                            }
                        }
                    }
                }
            }
        }
        """
    )

    params = {"id": org_id}

    # run query
    result = client.execute(query, params)

    for bg in result["organization"]["billingGroups"]["edges"]:
        name = bg["node"]["name"]
        id = bg["node"]["id"]
        billing_groups[id] = {"name": name}

    return billing_groups


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

    client = create_gql_client(api_key=token)

    job_id = None

    try:
        # get study and org ids
        study_id = get_study_id(client, study_name)
        org_id = get_org_id_from_study(client, study_id)

        # get billing group id
        billing_group_id = get_billing_id(client, org_id, billing)

        # check if volume loaded to study
        study_volumes = get_study_volumes(client, study_id)
        volume_id = process_volumes(study_id, study_volumes, vname=bucket_name)

        if volume_id is None:
            # need to load, get credential
            aws_cred_id = get_cred_id(client, study_id, aws_cred)

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
    """Main function. Call load_and_hash and output job_id."""
    job_id = load_and_hash_volume(
        args.bucket,
        args.study,
        args.region,
        args.prefix,
        args.billing,
        args.credential,
    )
    print(job_id)
