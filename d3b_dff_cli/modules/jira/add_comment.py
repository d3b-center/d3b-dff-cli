"""Add comment to Jira ticket"""
import json
import urllib3
import logging

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
    
    return


def add_jira_comment(auth, jira_url, message, ticket_id):
    """
    Update Jira ticket with message and set status

    Parameters:
    auth (str): Base64 encoded Jira username and password
    jira_url (str): Jira URL
    message (str): Comment to add to Jira ticket
    ticket_id (str): Jira ticket ID
    """

    http = urllib3.PoolManager()

    def check_status(response):
        """
        Check status code of response
        """
        if response.status != 201:
            print(f"Error: {response.status} - {response.data}")
            exit(1)

        return response.status
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {auth}",
    }

    url = f"{jira_url}/rest/api/3/issue/{ticket_id}/comment"

    payload = json.dumps(
        {
            "body": {
                "content": [
                    {
                        "content": [{"text": f"{message}", "type": "text"}],
                        "type": "paragraph",
                    }
                ],
                "type": "doc",
                "version": 1,
            }
        }
    ).encode("utf-8")

    response = http.request("POST", url, body=payload, headers=headers)

    check_status(response)

    return
