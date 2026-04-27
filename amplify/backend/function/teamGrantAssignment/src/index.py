import os
import time
import random
import boto3
from botocore.exceptions import ClientError

IDC_REGION = os.environ.get("IDC_REGION") or os.environ.get("AWS_REGION")

RETRYABLE_ERROR_CODES = {"ConflictException", "ThrottlingException", "TooManyRequestsException"}
MAX_ATTEMPTS = 6
BASE_DELAY_SECONDS = 2
MAX_DELAY_SECONDS = 15


def _serialise_status(status):
    if not status:
        return {}
    out = dict(status)
    if out.get("CreatedDate"):
        out["CreatedDate"] = out["CreatedDate"].isoformat()
    return out


def _backoff_seconds(attempt):
    delay = min(MAX_DELAY_SECONDS, BASE_DELAY_SECONDS * (2 ** (attempt - 1)))
    return delay + random.uniform(0, 1)


def handler(event, context):
    client = boto3.client("sso-admin", region_name=IDC_REGION)
    last_error = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
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
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            last_error = exc
            if code in RETRYABLE_ERROR_CODES and attempt < MAX_ATTEMPTS:
                time.sleep(_backoff_seconds(attempt))
                continue
            raise
    raise last_error
