import json
import logging
import os
from datetime import datetime, timezone

import requests
from falconpy import Alerts
from requests.auth import HTTPBasicAuth

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
JIRA_USER = os.environ.get("JIRA_USER")
JIRA_TOKEN = os.environ.get("JIRA_TOKEN")
FALCON_CLIENT_ID = os.environ.get("FALCON_CLIENT_ID")
FALCON_CLIENT_SECRET = os.environ.get("FALCON_CLIENT_SECRET")
ATL_COMPANY_DOMAIN = os.environ.get("ATL_COMPANY_DOMAIN", "")
JIRA_PROJECT_NAME = os.environ.get("JIRA_PROJECT_NAME", "")
JIRA_TRANSITION_ID = os.environ.get("JIRA_TRANSITION_ID", "4")
MAX_ALERTS = int(os.environ.get("MAX_ALERTS", "5"))

# Validate required environment variables
required_vars = ["JIRA_USER", "JIRA_TOKEN", "FALCON_CLIENT_ID", "FALCON_CLIENT_SECRET", 
                "ATL_COMPANY_DOMAIN", "JIRA_PROJECT_NAME"]
missing_vars = [var for var in required_vars if not locals()[var]]
if missing_vars:
    raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Initialize API clients
falcon = Alerts(client_id=FALCON_CLIENT_ID, client_secret=FALCON_CLIENT_SECRET)
auth = HTTPBasicAuth(JIRA_USER, JIRA_TOKEN)


def jira_fetch(alerts_data):
    """
    Fetch Jira issues related to CrowdStrike alerts and map them.
    
    Args:
        alerts_data (list): List of dictionaries containing alert information.
        
    Returns:
        list: Mapped data containing Jira issues and related alert information.
    """
    url = f"https://{ATL_COMPANY_DOMAIN}.atlassian.net/rest/api/3/search"
    headers = {"Accept": "application/json"}
    mapped_data = []
    
    for alert in alerts_data:
        query = {
            'jql': f'project = {JIRA_PROJECT_NAME} AND status IN ("In Progress", "To Do") AND description ~ "{alert["alert_id"]}" ORDER BY created DESC',
            'fields': 'key'
        }
        
        try:
            response = requests.get(url, headers=headers, params=query, auth=auth, timeout=10)
            logger.info(f"Jira search status code: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                issue_keys = [issue["key"] for issue in data.get("issues", [])]
                
                if issue_keys:
                    for issue_key in issue_keys:
                        mapped_data.append({
                            "issue_key": issue_key,
                            "assignee_email": alert["assignee_email"],
                            "comments": alert.get("comments", []),
                            "alert_id": alert["alert_id"]
                        })
                else:
                    logger.info(f"No Jira issues found for alert ID: {alert['alert_id']}")
            else:
                logger.error(f"Error querying Jira: {response.status_code}, Response: {response.text}")
        except Exception as e:
            logger.error(f"Exception during Jira fetch: {str(e)}")
    
    logger.info(f"Found {len(mapped_data)} Jira issues to update")
    
    if mapped_data:
        change_jira_status(mapped_data)
        sync_comments(mapped_data)
    
    return mapped_data


def change_jira_status(issues_data):
    """
    Update Jira issue status and assignee.
    
    Args:
        issues_data (list): List of dictionaries containing issue information.
    """
    change_jira_issue_assignee(issues_data)
    
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    payload = json.dumps({
        "transition": {
            "id": JIRA_TRANSITION_ID
        }
    })
    
    for issue in issues_data:
        url = f"https://{ATL_COMPANY_DOMAIN}.atlassian.net/rest/api/3/issue/{issue['issue_key']}/transitions"
        try:
            response = requests.post(url, data=payload, headers=headers, auth=auth, timeout=10)
            
            if response.status_code != 204:
                logger.error(f"Error while changing status of {issue['issue_key']}: {response.status_code}")
                if hasattr(response, 'text'):
                    logger.error(f"Response: {response.text}")
        except Exception as e:
            logger.error(f"Exception during status change for {issue['issue_key']}: {str(e)}")


def falcon_fetch():
    """
    Fetch closed alerts from CrowdStrike.
    
    Returns:
        list: List of alert data including comments.
    """
    try:
        response = falcon.query_alerts_v2(
            filter="status:'closed'",
            sort="created_timestamp.desc",
            limit=MAX_ALERTS,
            include_hidden=False
        )
        
        alert_ids = response.get("body", {}).get("resources", [])
        
        if not alert_ids:
            logger.info("No alert IDs found in CrowdStrike response")
            return []
        
        logger.info(f"Found {len(alert_ids)} alert IDs from CrowdStrike")
        
        detail_response = falcon.get_alerts_v2(composite_ids=alert_ids)
        detail_alerts = detail_response.get("body", {}).get("resources", [])
        
        if not detail_alerts:
            logger.info("No alert details found in CrowdStrike response")
            return []
        
        # Extract alert data including comments
        alert_data = []
        for alert in detail_alerts:
            alert_data.append({
                "alert_id": alert.get("composite_id", "N/A"),
                "assignee_email": alert.get("assigned_to_uid", "Unassigned"),
                "comments": alert.get("comments", [])
            })
        
        logger.info(f"Processed {len(alert_data)} alerts with details")
        return alert_data
    
    except Exception as e:
        logger.error(f"Exception during CrowdStrike fetch: {str(e)}")
        return []


def change_jira_issue_assignee(mapped_data):
    """
    Update the assignee of Jira issues.
    
    Args:
        mapped_data (list): List of dictionaries containing issue and assignee information.
    """
    for issue in mapped_data:
        try:
            account_id = find_jira_account_id(issue["assignee_email"])
            
            if not account_id:
                logger.warning(f"Could not find Jira account ID for {issue['assignee_email']}")
                continue
            
            url = f"https://{ATL_COMPANY_DOMAIN}.atlassian.net/rest/api/3/issue/{issue['issue_key']}/assignee"
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json"
            }
            payload = json.dumps({"accountId": account_id})
            
            response = requests.put(url, data=payload, headers=headers, auth=auth, timeout=10)
            
            if response.status_code != 204:
                logger.error(f"Error while changing assignee for {issue['issue_key']}: {response.status_code}")
                logger.error(f"Response: {response.text}")
        except Exception as e:
            logger.error(f"Exception during assignee change for {issue['issue_key']}: {str(e)}")


def find_jira_account_id(assignee_email):
    """
    Find the Jira account ID for a given email.
    
    Args:
        assignee_email (str): Email address of the assignee.
        
    Returns:
        str: Jira account ID or None if not found.
    """
    if not assignee_email or assignee_email == "Unassigned":
        return None
    
    try:
        url = f"https://{ATL_COMPANY_DOMAIN}.atlassian.net/rest/api/3/user/search?query={assignee_email}"
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        
        response = requests.get(url, headers=headers, auth=auth, timeout=10)
        
        if response.status_code == 200:
            json_resp = json.loads(response.text)
            if json_resp and len(json_resp) > 0:
                return json_resp[0]["accountId"]
        else:
            logger.error(f"Error finding account ID for {assignee_email}: {response.status_code}")
    except Exception as e:
        logger.error(f"Exception during account ID lookup for {assignee_email}: {str(e)}")
    
    return None


def get_jira_comments(issue_key):
    """
    Fetch existing comments from a Jira issue.
    
    Args:
        issue_key (str): Jira issue key.
        
    Returns:
        list: List of comments.
    """
    try:
        url = f"https://{ATL_COMPANY_DOMAIN}.atlassian.net/rest/api/3/issue/{issue_key}/comment"
        headers = {"Accept": "application/json"}
        
        response = requests.get(url, headers=headers, auth=auth, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            return data.get("comments", [])
        else:
            logger.error(f"Error fetching Jira comments for {issue_key}: {response.status_code}")
    except Exception as e:
        logger.error(f"Exception fetching comments for {issue_key}: {str(e)}")
    
    return []


def parse_timestamp(timestamp_str):
    """
    Parse CrowdStrike timestamp string to Unix timestamp.
    
    Args:
        timestamp_str (str): Timestamp string in CrowdStrike format.
        
    Returns:
        float: Unix timestamp.
    """
    try:
        # Handle timezone-aware timestamp format
        dt = datetime.strptime(timestamp_str.split('.')[0], "%Y-%m-%dT%H:%M:%S")
        dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception as e:
        logger.error(f"Error parsing timestamp {timestamp_str}: {str(e)}")
        return 0


def add_jira_comment(issue_key, comment_text, author_email, timestamp_str):
    """
    Add a comment to a Jira issue with attribution to the original author.
    
    Args:
        issue_key (str): Jira issue key.
        comment_text (str): Comment content.
        author_email (str): Email of the original author.
        timestamp_str (str): Timestamp string.
        
    Returns:
        bool: Success status.
    """
    # Parse the timestamp string from CrowdStrike
    timestamp = parse_timestamp(timestamp_str)
    formatted_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
    
    # Create a formatted comment with Atlassian Document Format
    formatted_comment = {
        "body": {
            "type": "doc",
            "version": 1,
            "content": [
                {
                    "type": "paragraph",
                    "content": [
                        {
                            "type": "text",
                            "text": f"Comment from CrowdStrike by {author_email} on {formatted_time}:",
                            "marks": [
                                {
                                    "type": "strong"
                                }
                            ]
                        }
                    ]
                },
                {
                    "type": "paragraph",
                    "content": [
                        {
                            "type": "text",
                            "text": comment_text
                        }
                    ]
                }
            ]
        }
    }
    
    try:
        url = f"https://{ATL_COMPANY_DOMAIN}.atlassian.net/rest/api/3/issue/{issue_key}/comment"
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        
        response = requests.post(url, json=formatted_comment, headers=headers, auth=auth, timeout=10)
        
        if response.status_code == 201:
            logger.info(f"Comment added to Jira issue {issue_key}")
            return True
        else:
            logger.error(f"Error adding comment to Jira issue {issue_key}: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return False
    except Exception as e:
        logger.error(f"Exception adding comment to {issue_key}: {str(e)}")
        return False


def sync_comments(mapped_data):
    """
    Sync comments from CrowdStrike alerts to corresponding Jira issues.
    
    Args:
        mapped_data (list): List of mapped alert and issue data.
    """
    for item in mapped_data:
        jira_key = item["issue_key"]
        comments = item.get("comments", [])
        
        if not comments:
            logger.info(f"No comments found for alert related to Jira issue {jira_key}")
            continue
        
        logger.info(f"Processing {len(comments)} comments for Jira issue {jira_key}")
        
        # Fetch existing Jira comments
        jira_comments = get_jira_comments(jira_key)
        
        # Extract existing comment timestamps and values to avoid duplication
        existing_comments = set()
        for comment in jira_comments:
            body = comment.get("body", {})
            comment_text = ""
            
            # Extract the full comment text to check for duplicates
            if isinstance(body, dict) and "content" in body:
                for content in body["content"]:
                    if content.get("type") == "paragraph" and "content" in content:
                        for text in content["content"]:
                            if text.get("type") == "text":
                                comment_text += text.get("text", "")
            
            # Add this to our set of existing comments
            existing_comments.add(comment_text)
        
        # Counter for synced comments
        synced_count = 0
        
        # Add new comments to Jira
        for comment in comments:
            author_email = comment.get("falcon_user_id", "Unknown User")
            timestamp = comment.get("timestamp", "")
            comment_text = comment.get("value", "")
            
            if not comment_text:
                continue
                
            # Create the comment text as it would appear in Jira
            formatted_time = "Unknown Date"
            if timestamp:
                timestamp_unix = parse_timestamp(timestamp)
                formatted_time = datetime.fromtimestamp(timestamp_unix).strftime("%Y-%m-%d %H:%M:%S")
                
            # This is a simplified check - we're looking for comments that contain both the timestamp and the comment text
            potential_comment = f"Comment from CrowdStrike by {author_email} on {formatted_time}:"
            
            # Skip if we find a match (this is an approximate check)
            skip = False
            for existing in existing_comments:
                if potential_comment in existing and comment_text in existing:
                    skip = True
                    break
            
            if skip:
                logger.info(f"Skipping comment that appears to already exist in Jira")
                continue
                
            # Add the comment to Jira
            success = add_jira_comment(jira_key, comment_text, author_email, timestamp)
            if success:
                synced_count += 1
        
        logger.info(f"Synced {synced_count} new comments to Jira issue {jira_key}")


def lambda_handler(event, context):
    """
    AWS Lambda handler function.
    
    Args:
        event (dict): Lambda event data.
        context (object): Lambda context object.
        
    Returns:
        dict: Response with execution status.
    """
    logger.info("Starting CrowdStrike to Jira synchronization")
    
    try:
        alerts_data = falcon_fetch()
        
        if alerts_data:
            logger.info(f"Processing {len(alerts_data)} alerts from CrowdStrike")
            jira_issues = jira_fetch(alerts_data)
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "Synchronization completed successfully",
                    "alerts_processed": len(alerts_data),
                    "jira_issues_updated": len(jira_issues)
                })
            }
        else:
            logger.info("No alerts found to process")
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "No alerts found to process"
                })
            }
    except Exception as e:
        logger.error(f"Error during synchronization: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": f"Error during synchronization: {str(e)}"
            })
        }


if __name__ == "__main__":
    # For local testing
    lambda_handler(None, None)