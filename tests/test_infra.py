import os
import unittest

os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import boto3  # noqa: E402
from moto import mock_aws  # noqa: E402

from fleet import infra  # noqa: E402


class TestTemplate(unittest.TestCase):
    def test_template_structure(self):
        t = infra.load_template()
        res = t["Resources"]
        self.assertEqual(res["TelemetryQueue"]["Type"], "AWS::SQS::Queue")
        self.assertEqual(res["DeadLetterQueue"]["Type"], "AWS::SQS::Queue")
        self.assertEqual(res["RobotStateTable"]["Type"], "AWS::DynamoDB::Table")
        redrive = res["TelemetryQueue"]["Properties"]["RedrivePolicy"]
        self.assertEqual(redrive["deadLetterTargetArn"], {"Fn::GetAtt": ["DeadLetterQueue", "Arn"]})
        self.assertEqual(set(t["Outputs"]), {"QueueUrl", "DeadLetterQueueUrl", "TableName"})

    @mock_aws
    def test_stack_deploys(self):
        cfn = boto3.client("cloudformation")
        out = infra.deploy_stack(cfn, stack_name="t", prefix="unit", visibility_timeout=7)
        sqs = boto3.client("sqs")
        attrs = sqs.get_queue_attributes(QueueUrl=out["QueueUrl"], AttributeNames=["All"])["Attributes"]
        self.assertEqual(attrs["VisibilityTimeout"], "7")
        policy = infra.redrive_policy(sqs, out["QueueUrl"])
        self.assertEqual(int(policy["maxReceiveCount"]), 3)
        self.assertTrue(policy["deadLetterTargetArn"].endswith(":unit-telemetry-dlq"))
        table = boto3.client("dynamodb").describe_table(TableName=out["TableName"])["Table"]
        self.assertEqual(table["KeySchema"], [{"AttributeName": "robot_id", "KeyType": "HASH"}])
        # Deploying again reuses the stack instead of failing.
        self.assertEqual(infra.deploy_stack(cfn, stack_name="t", prefix="unit"), out)

    @mock_aws
    def test_redrive_moves_only_accepted(self):
        sqs = boto3.client("sqs")
        q = sqs.create_queue(QueueName="q")["QueueUrl"]
        dlq = sqs.create_queue(QueueName="dlq")["QueueUrl"]
        for body in ["keep-1", "keep-2", "bad"]:
            sqs.send_message(QueueUrl=dlq, MessageBody=body)
        moved, left = infra.redrive(sqs, dlq, q, accept=lambda b: b.startswith("keep"))
        self.assertEqual((moved, left), (2, 1))
        self.assertEqual(sorted(m["Body"] for m in infra.drain_queue(sqs, q, wait_s=0)), ["keep-1", "keep-2"])


if __name__ == "__main__":
    unittest.main()
