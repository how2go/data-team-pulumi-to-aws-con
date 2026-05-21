"""
Label Extraction Pipeline — Pulumi infrastructure definition.

Deploys one Lambda + one EventBridge rule:
  - label-extraction-lambda
      Reads: healf.label_extraction.int_shopify_metadata__enriched_product_variants
      Writes: s3://shopify-products-metadata/edible/{YYYY-MM-DD_HH:MM:SS}/enriched_variants.csv
      Fires at 06:00 UTC daily via EventBridge.
      If the source table is empty, nothing is written to S3.

Deploy: push to the label-extraction-pipeline branch — GitHub Actions runs pulumi up.
"""

import os
import json
import pulumi
import pulumi_aws as aws

BUCKET_NAME = "shopify-products-metadata"
SCHEDULE = "cron(0 6 * * ? *)"  # 06:00 UTC daily

# Snowflake private key is injected by GitHub Actions as an env var at deploy time.
# The value is stored as a GitHub secret (SNOWFLAKE_PRIVATE_KEY) and passed to the
# Lambda's environment so it never touches the Pulumi state in plain text.
snowflake_private_key = os.environ.get("SNOWFLAKE_PRIVATE_KEY", "")

# ---------------------------------------------------------------------------
# IAM role for the Lambda
# ---------------------------------------------------------------------------
role = aws.iam.Role(
    "label-extraction-role",
    assume_role_policy=json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Action": "sts:AssumeRole",
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
        }],
    }),
)

aws.iam.RolePolicy(
    "label-extraction-policy",
    role=role.id,
    policy=json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                    "s3:GetObject",
                    "s3:ListBucket",
                ],
                "Resource": [
                    f"arn:aws:s3:::{BUCKET_NAME}",
                    f"arn:aws:s3:::{BUCKET_NAME}/*",
                ],
            },
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                "Resource": "arn:aws:logs:*:*:*",
            },
        ],
    }),
)

# ---------------------------------------------------------------------------
# Lambda function
# ---------------------------------------------------------------------------
label_lambda = aws.lambda_.Function(
    "label-extraction-lambda",
    code=pulumi.AssetArchive({".": pulumi.FileArchive("./src/label_extraction_lambda")}),
    handler="handler.main",
    runtime="python3.11",
    role=role.arn,
    timeout=300,       # 5 minutes max; data transfer should complete in ~2 min
    memory_size=512,
    environment={"variables": {
        "SNOWFLAKE_USER":      "SJ_SERVICE_USER",
        "SNOWFLAKE_ACCOUNT":   "GWNDCGK-GN77379",
        "SNOWFLAKE_WAREHOUSE": "HEALF_WH",
        "SNOWFLAKE_DATABASE":  "HEALF",
        "SNOWFLAKE_SCHEMA":    "label_extraction",
        "SNOWFLAKE_ROLE":      "PC_THOUGHTSPOT_ROLE",
        "S3_BUCKET_NAME":      BUCKET_NAME,
        "SNOWFLAKE_PRIVATE_KEY": snowflake_private_key,
    }},
)

# ---------------------------------------------------------------------------
# EventBridge rule — fires at 06:00 UTC every day
# ---------------------------------------------------------------------------
event_rule = aws.cloudwatch.EventRule(
    "label-extraction-daily",
    schedule_expression=SCHEDULE,
    description="Fires the label extraction Lambda at 06:00 UTC daily.",
)

aws.cloudwatch.EventTarget(
    "label-extraction-target",
    rule=event_rule.name,
    arn=label_lambda.arn,
)

aws.lambda_.Permission(
    "label-extraction-perm",
    action="lambda:InvokeFunction",
    function=label_lambda.name,
    principal="events.amazonaws.com",
    source_arn=event_rule.arn,
)

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------
pulumi.export("lambda_arn", label_lambda.arn)
pulumi.export("event_rule_name", event_rule.name)
pulumi.export("s3_output_prefix", f"s3://{BUCKET_NAME}/edible/")
