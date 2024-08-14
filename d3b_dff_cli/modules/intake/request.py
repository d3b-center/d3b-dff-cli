"""Create data intake epic in Jira"""

import json
import urllib3
import logging
import time
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def check_status(response):
    """
    Check response status and handle appropriately
    """

    # check that response is successful
    if response.status in [201, 204]:
        logger.info("Jira ticket updated successfully")
    elif response.status == 200:
        logger.info("Get request successful")
    else:
        logger.error(
            f"Failed to update Jira ticket: {response.status} - {response.data}"
        )
        exit(1)

    return


def convert_keys_to_id(key, field, meta_res):
    """
    Convert text key to value id.
    Set default values based on "Unknown" or similar value for field.
    Loop through allowed values to find match and return id.

    Inputs:
    - key (str): Text key to convert
    - field (str): Field name
    - meta_res (urllib3.response.HTTPResponse): Response from Jira API

    Returns:
    - field_id (str): ID of allowed value
    """

    # set up defaults
    default_ids = {
        "Study": "10412",  # Unknown
        "Data Source": "10413",  # Source Not Listed - Please Add New Source
        "Program": "10380",  # D3B
    }

    field_id = default_ids[field]

    meta_data = json.loads(meta_res.data)

    for ticket_field in meta_data["fields"]:
        if ticket_field["name"] == field:
            for option in ticket_field["allowedValues"]:
                if option["value"] == key:
                    field_id = option["id"]

    return field_id


def intake_request(args):
    """
    Create data intake epic and return epic and transfer ticket ids.

    Input:
    - args (argparse.Namespace): Parsed command-line arguments

    Output:
    - epic_id (str): Epic ID
    - transfer_id (str): Transfer ID
    """

    # jira internal id for the data intake epic
    intake_epic_id = 10231

    # jira internal id for AD project
    project_id = 10147

    epic_key, transfer_key = ["AD-0", "AD-0"]

    http = urllib3.PoolManager()

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {args.auth}",
    }

    url = f"{args.jira_url}/rest/api/3/issue/"

    # Required cutom fields:
    study_field_key = "customfield_10136"
    data_source_field_key = "customfield_10139"
    program_field_key = "customfield_10140"

    # translate between text and custom field allowed value id

    # get issue type fields and required values
    meta_url = f"{args.jira_url}/rest/api/3/issue/createmeta/{project_id}/issuetypes/{intake_epic_id}"
    meta_res = http.request("GET", meta_url, headers=headers)
    check_status(meta_res)

    # convert from text to allowed value id
    study_id = convert_keys_to_id(args.study, "Study", meta_res)
    data_source_id = convert_keys_to_id(args.data_source, "Data Source", meta_res)
    program_id = convert_keys_to_id(args.program, "Program", meta_res)

    date = datetime.now()

    # override default summary if args.summary provided
    summary = None
    if args.summary:
        summary = args.summary
    else:
        summary = f"{args.study} data intake from {args.data_source} - {date}"

    # add test label if not prd
    if not args.prd:
        summary = f"TEST {summary}"

    # build post json
    payload = json.dumps(
        {
            "fields": {
                "project": {"id": project_id},
                "issuetype": {"id": intake_epic_id},
                "summary": summary,
                study_field_key: [{"id": study_id}],
                data_source_field_key: {"id": data_source_id},
                program_field_key: {"id": program_id},
            }
        }
    ).encode("utf-8")

    # if args.post, post and make epic
    if args.post:
        response = http.request("POST", url, body=payload, headers=headers)

        check_status(response)

        # extract epic id and transfer id
        epic_key = json.loads(response.data)["key"]

        # get transfer key from epic key
        epic_proj, epic_num = epic_key.split("-")
        transfer_key = f"{epic_proj}-{int(epic_num)+1}"

        # wait before checking if transfer ticket exists
        time.sleep(5)

        # query that transfer ticket exists (extra check that this worked)
        ticket_url = f"{args.jira_url}/rest/api/3/issue/{transfer_key}"
        response = http.request("GET", ticket_url, headers=headers)
        check_status(response)

    else:
        print("Dry run, no request submitted")
        print("-----------------------------")
        print(json.dumps(json.loads(payload), sort_keys=True))
        print("-----------------------------")

    return epic_key, transfer_key


def main(args):
    """Main function."""
    epic_key, transfer_key = intake_request(args)

    print(f"Epic ID: {epic_key}")
    print(f"Transfer Ticket ID: {transfer_key}")

    return
