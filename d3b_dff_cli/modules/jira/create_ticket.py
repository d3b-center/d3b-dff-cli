"""Create Jira ticket with allowed values"""

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


def get_project_issue_type_ids(project, issue_type, jira_url, headers):
    """
    Get issue type id from project. Also checks if project exists.

    Inputs:
    - project (str): Jira project name
    - issue_type (str): Jira issue type name
    - jira_url (str): Jira base URL
    - headers (dict): HTTP headers for Jira API requests

    Returns:
    - project_id (str): project id
    - issue_type_id (str): issue type id
    """

    project_id = None
    issue_type_id = None

    # check if project exists
    http = urllib3.PoolManager()
    url = f"{jira_url}/rest/api/3/project/{project}"
    res = http.request("GET", url, headers=headers)
    check_status(res)

    data = json.loads(res.data)

    project_id = data["id"]

    # loop through issue types and check if issue type exists in project
    for it in data["issueTypes"]:
        if it["name"] == issue_type:
            issue_type_id = it["id"]

    return project_id, issue_type_id


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

    field_id = None

    meta_data = json.loads(meta_res.data)

    for ticket_field in meta_data["fields"]:
        if ticket_field["name"] == field:
            for option in ticket_field["allowedValues"]:
                if option["value"] == key:
                    field_id = option["id"]

    return field_id


def convert_username_to_id(username, jira_url, headers):
    """
    Convert from Jira username to id

    Params:
    - username (str): Jira email or displayName in a pinch
    - jira_url (str): Jira base URL
    - headers (dict): HTTP headers for Jira API requests

    Returns:
    - user_id (str): Jira user id
    """

    http = urllib3.PoolManager()

    user_id = None

    # if the username contains an at, use email else print warning and use displayName
    search_field = "emailAddress"
    if not "@" in username:
        logger.warning(
            "Assuming username is displayName. displayName is not unique, ensure you're using the correct user"
        )
        search_field = "displayName"

    last_page = False
    page = 0
    while last_page == False:

        user_url = f"{jira_url}/rest/api/3/users/search?startAt={page * 50}"
        res = http.request("GET", user_url, headers=headers)
        check_status(res)

        data = json.loads(res.data)

        # look for user
        for user in data:
            if (
                search_field in user
                and user[search_field] == username
                and user["active"] == True
            ):
                user_id = user["accountId"]
                last_page = True

        if len(data) < 50:
            last_page = True
        else:
            page += 1

    if user_id is None:
        message = f"User {username} not found in Jira"
        logger.error(message)
        raise ValueError(message)

    return user_id


def create_ticket(project_id, issue_type_id, fields, post, prd, jira_url, headers):
    """
    Create Jira ticket with provided fields

    Input:
    - project_id (str): Jira project id
    - issue_type_id (str): Jira issue type id
    - fields (dict): Dictionary of issue fields
    - post (bool): If true, actually post request
    - prd (bool): If true, remove TEST from summary
    - jira_url (str): Jira base URL
    - headers (dict): HTTP headers for Jira API requests

    Output:
    - ticket_key (str): Ticket key (colloquially called Ticket ID)
    """

    ticket_key = None

    http = urllib3.PoolManager()

    url = f"{jira_url}/rest/api/3/issue/"

    date = datetime.now()

    payload = None

    # figure out what fields are in the issue being created
    meta_url = f"{jira_url}/rest/api/3/issue/createmeta/{project_id}/issuetypes/{issue_type_id}"
    meta_res = http.request("GET", meta_url, headers=headers)
    check_status(meta_res)

    # turn ticket fields to dict
    fields_json = json.loads(meta_res.data)["fields"]
    ticket_fields = {}
    for ticket_field in fields_json:
        name = ticket_field["name"]
        ticket_fields[name] = {}
        for key in ticket_field:
            ticket_fields[name][key] = ticket_field[key]

    # build default summary
    if not fields.get("Summary"):
        # will need guidance on what this should be generically
        # also need to add checks that these other fields exist
        fields["Summary"] = (
            f"{fields['Study']} data intake from {fields['Data Source']} - {date}"
        )

    # loop through fields and associate fields with field id in issue
    formatted_fields = {}
    for field in fields:
        if field == "Summary":
            if not prd:
                fields[field] = f"TEST {fields[field]}"

        # check that field exists in issue type
        if not field in ticket_fields:
            message = f"Field {field} not found in issue type"
            logger.error(message)
            raise ValueError(message)

        # format field data
        # 6 types of fields for now
        # will we need to split custom field ids from regular ones?
        # might not need content dict, try to set description with just key: value
        # [X] key: value
        # [X] key: list of values
        # [P] key: id (this is done for allowed values and users)
        # [P] key: list of ids (see above)
        # [P] key: cotent dict (only for description)
        # [X] parent: key: ticket_id

        # need to figure out which issues need ids and how to convert between name and id...
        my_key = ticket_fields[field]["key"]
        if my_key == "parent":
            formatted_fields[my_key] = {"key": fields[field]}
        if ticket_fields[field]["schema"]["type"] == "array":
            my_list = fields[field].split(",")
            if ticket_fields[field]["schema"]["items"] == "option":
                my_list = [
                    {"id": convert_keys_to_id(item, field, meta_res)}
                    for item in my_list
                ]
            elif ticket_fields[field]["schema"]["items"] == "user":
                my_list = [
                    {"id": convert_username_to_id(item, jira_url, headers)}
                    for item in my_list
                ]
            formatted_fields[my_key] = my_list
        else:
            if ticket_fields[field]["schema"]["type"] == "option":
                formatted_fields[my_key] = {
                    "id": convert_keys_to_id(fields[field], field, meta_res)
                }
            elif ticket_fields[field]["schema"]["type"] == "user":
                formatted_fields[my_key] = {
                    "id": convert_username_to_id(fields[field], jira_url, headers)
                }
            elif field == "Description":
                formatted_fields[my_key] = {
                    "content": [
                        {
                            "content": [{"text": fields[field], "type": "text"}],
                            "type": "paragraph",
                        }
                    ],
                    "type": "doc",
                    "version": 1,
                }
            else:
                formatted_fields[my_key] = fields[field]

    # build the payload to post
    formatted_fields["project"] = {"id": project_id}
    formatted_fields["issuetype"] = {"id": issue_type_id}
    payload = json.dumps({"fields": formatted_fields}).encode("utf-8")

    # if post, post and make epic
    if post:
        response = http.request("POST", url, body=payload, headers=headers)

        check_status(response)

        # extract epic id and transfer id
        ticket_key = json.loads(response.data)["key"]

        # wait before checking if transfer ticket exists
        time.sleep(5)

    else:
        print("Dry run, no request submitted")
        print("-----------------------------")
        print(json.dumps(json.loads(payload), sort_keys=True))
        print("-----------------------------")

    return ticket_key


def get_transfer_key(epic_key, headers, jira_url):
    """
    Get transfer ticket id from epic.

    Input:
    - epic_key (str): Epic ticket key
    - headers (dict): HTTP headers for Jira API requests
    - jira_url (str): Base URL for Jira instance

    Output:
    - transfer_key (str): Transfer ticket key
    """

    transfer_key = None

    http = urllib3.PoolManager()

    # find issues where the parent is the epic_key
    query_url = f"{jira_url}/rest/api/3/search?jql=(parent = {epic_key})"

    response = http.request("GET", query_url, headers=headers)
    check_status(response)

    query_data = json.loads(response.data)

    # loop through fields to find transfer ticket
    for issue in query_data["issues"]:
        if "Data Transfer - " in issue["fields"]["summary"]:
            transfer_key = issue["key"]

    if transfer_key is None:
        logger.warning("Transfer ticket not found")

    return transfer_key


def main(args):
    """Main function."""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {args.auth}",
    }

    # get issue type id from project
    project_id, issue_type_id = get_project_issue_type_ids(
        args.project, args.issue_type, args.jira_url, headers
    )

    if issue_type_id is None:
        message = f"Issue type {args.issue_type} not found in project {args.project}"
        logger.error(message)
        raise ValueError(message)

    # build fields dict from args.fields
    fields = json.loads(args.fields)

    ticket_key = create_ticket(
        project_id, issue_type_id, fields, args.post, args.prd, args.jira_url, headers
    )

    output_json = None

    if args.post:
        if args.issue_type == "Data Intake Epic":
            transfer_key = get_transfer_key(ticket_key, headers, args.jira_url)
            output_json = {"Epic ID": ticket_key, "Transfer Ticket ID": transfer_key}

        else:
            output_json = f"Ticket ID: {ticket_key}"

        print(json.dumps(output_json, indent=4, sort_keys=True))

    return
