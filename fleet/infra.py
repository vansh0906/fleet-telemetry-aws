"""Deploys the CloudFormation stack (SQS + DLQ + DynamoDB) and handles dead-letter redrive."""
from __future__ import annotations

import json
import time
from pathlib import Path

import yaml

TEMPLATE = Path(__file__).resolve().parent.parent / "infra" / "fleet-stack.yaml"


def load_template() -> dict:
    """The template uses plain JSON-style intrinsics ({"Ref": ...}) so a normal YAML parser reads it."""
    return yaml.safe_load(TEMPLATE.read_text())


def deploy_stack(cfn, stack_name: str = "fleet-telemetry", prefix: str = "fleet",
                 visibility_timeout: int = 30, max_receive_count: int = 3, timeout_s: float = 120) -> dict:
    """Creates the stack (or reuses an existing one) and returns its outputs."""
    params = [
        {"ParameterKey": "Prefix", "ParameterValue": prefix},
        {"ParameterKey": "VisibilityTimeout", "ParameterValue": str(visibility_timeout)},
        {"ParameterKey": "MaxReceiveCount", "ParameterValue": str(max_receive_count)},
    ]
    existing = [s for s in cfn.list_stacks()["StackSummaries"]
                if s["StackName"] == stack_name and s["StackStatus"] != "DELETE_COMPLETE"]
    if not existing:
        cfn.create_stack(StackName=stack_name, TemplateBody=TEMPLATE.read_text(), Parameters=params)
    deadline = time.time() + timeout_s
    while True:
        stack = cfn.describe_stacks(StackName=stack_name)["Stacks"][0]
        status = stack["StackStatus"]
        if status.endswith("_COMPLETE") and "ROLLBACK" not in status:
            return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
        if "FAILED" in status or "ROLLBACK" in status:
            raise RuntimeError(f"stack {stack_name}: {status}")
        if time.time() > deadline:
            raise TimeoutError(f"stack {stack_name} still {status}")
        time.sleep(1)


def queue_depth(sqs, url: str) -> tuple[int, int]:
    """(messages waiting, messages received but not yet deleted)."""
    attrs = sqs.get_queue_attributes(
        QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"]
    )["Attributes"]
    return int(attrs["ApproximateNumberOfMessages"]), int(attrs["ApproximateNumberOfMessagesNotVisible"])


def redrive_policy(sqs, url: str) -> dict:
    attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["RedrivePolicy"])["Attributes"]
    return json.loads(attrs.get("RedrivePolicy", "{}"))


def drain_queue(sqs, url: str, wait_s: float = 1.0) -> list[dict]:
    """Receives and deletes everything in a queue (used to inspect the dead-letter queue)."""
    out: list[dict] = []
    empty_polls = 0
    while empty_polls < 2:
        msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=int(wait_s),
                                   VisibilityTimeout=30).get("Messages", [])
        if not msgs:
            empty_polls += 1
            continue
        empty_polls = 0
        out.extend(msgs)
        sqs.delete_message_batch(QueueUrl=url, Entries=[
            {"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(msgs)])
    return out


def redrive(sqs, dlq_url: str, queue_url: str, accept=lambda body: True) -> tuple[int, int]:
    """Moves dead-lettered messages back to the main queue once the cause (say, a database outage) is fixed.
    `accept(body)` decides which ones go back; the others stay in the DLQ for a person to look at.
    Returns (moved back, left in the DLQ)."""
    moved = left = 0
    empty_polls = 0
    while empty_polls < 2:
        # Messages left in the DLQ stay hidden for 60 s, so this scan sees each message once.
        msgs = sqs.receive_message(QueueUrl=dlq_url, MaxNumberOfMessages=10, WaitTimeSeconds=1,
                                   VisibilityTimeout=60).get("Messages", [])
        if not msgs:
            empty_polls += 1
            continue
        empty_polls = 0
        for m in msgs:
            if accept(m["Body"]):
                sqs.send_message(QueueUrl=queue_url, MessageBody=m["Body"])
                sqs.delete_message(QueueUrl=dlq_url, ReceiptHandle=m["ReceiptHandle"])
                moved += 1
            else:
                left += 1
    return moved, left
