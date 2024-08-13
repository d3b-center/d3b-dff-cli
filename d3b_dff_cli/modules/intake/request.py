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

    # translate between text and custom field id
    # can we do this via query????
    # probably can, but not for MVP
    # rough plan:
    #   get metadata from https://d3b.atlassian.net/rest/api/3/issue/createmeta/10147/issuetypes/10231
    #   find name retrun id
    #   if not, return unknown (or something like unknown)

    study_id = "10412"  # Unknown Study
    if args.study == "DGD":
        study_id = "10298"

    data_source_id = "10413"  # Source Not Listed - Please Add New Source
    if args.data_source == "CHOP DGD":
        data_source_id = "10358"

    program_id = "10380"  # D3B
    if args.program == "CHOP":
        program_id = "10386"

    date = datetime.now()

    summary = None
    if args.summary:
        summary = args.summary
    else:
        summary = f"{args.study} data intake from {args.data_source} - {date}"

    if not args.prd:
        summary = f"TEST {summary}"

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