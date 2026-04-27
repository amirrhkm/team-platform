import os
import boto3
from botocore.exceptions import ClientError

IDC_REGION = os.environ.get("IDC_REGION") or os.environ.get("AWS_REGION")


def _serialise_status(status):
    if not status:
        return {}
    out = dict(status)
    if out.get("CreatedDate"):
        out["CreatedDate"] = out["CreatedDate"].isoformat()
    return out


def handler(event, context):
    client = boto3.client("sso-admin", region_name=IDC_REGION)
    try:
        response = client.create_account_assignment(
            InstanceArn=event["InstanceArn"],
            PermissionSetArn=event["PermissionSetArn"],
            PrincipalId=event["PrincipalId"],
            PrincipalType=event["PrincipalType"],
            TargetId=event["TargetId"],
            TargetType=event["TargetType"],
        )
        return {
            "AccountAssignmentCreationStatus": _serialise_status(
                response.get("AccountAssignmentCreationStatus")
            )
        }
    except ClientError:
        raise
