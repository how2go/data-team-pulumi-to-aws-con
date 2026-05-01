"""
Pulumi definition for the Shopify daily Lambdas.

Two Lambdas deployed:
  1. shopify-orders  -- orders/line-items/GXO/publications delta.
                        Fires at 03:00 UTC daily.
                        Handler: handler_orders.main
                        Output: s3://sources-data/shopify-data/shopify-orders/{run_date}/{run_date}.csv

  2. shopify-cogs    -- product/variant/publications snapshot.
                        Fires at 05:00 UTC daily.
                        Handler: handler_cogs.main
                        Output: s3://sources-data/shopify-data/shopify-cogs/{run_date}/{run_date}.csv

Daily orders mode:
  - handler_orders now fetches the full UTC day from 2 days ago by default
  - GXO fulfillment window is extended from order_start_dt to current UTC time

Backfill mode for handler_orders:
  - temporarily add BACKFILL_START + BACKFILL_END env vars
  - then remove them again after the backfill and re-deploy
"""

import json
import pulumi
import pulumi_aws as aws

config = pulumi.Config()
token = config.require_secret("SHOPIFY_ACCESS_TOKEN")
client_id = config.require("SHOPIFY_CLIENT_ID")

# --- 1. SCHEDULES ---
ORDERS_RATE = "cron(0 3 * * ? *)"  # 03:00 UTC daily
COGS_RATE = "cron(0 5 * * ? *)"    # 05:00 UTC daily

BUCKET_NAME = "sources-data"

# --- 2. SHARED IAM ROLE ---
role = aws.iam.Role(
    "shopify-data-role",
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
    "shopify-data-policy",
    role=role.id,
    policy=json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket",
                    "s3:AbortMultipartUpload",
                    "s3:ListMultipartUploadParts",
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

# --- 3. SHOPIFY ORDERS LAMBDA ---
# Daily mode = 2 days ago window, handled inside handler_orders.py
# Do not set BACKFILL_START / BACKFILL_END here for normal daily running.
orders_lambda = aws.lambda_.Function(
    "shopify-orders",
    code=pulumi.AssetArchive({".": pulumi.FileArchive("./src/shopify_lambda")}),
    handler="handler_orders.main",
    runtime="python3.11",
    role=role.arn,
    timeout=900,
    memory_size=10240,
    ephemeral_storage={"size": 10240},
    environment={"variables": {
        "SHOPIFY_ACCESS_TOKEN": token,
        "SHOPIFY_CLIENT_ID": client_id,
        "S3_BUCKET_NAME": BUCKET_NAME,
        "SHOP_NAME": "how2go",
    }},
)

# --- 4. SHOPIFY COGS LAMBDA ---
# Left untouched as requested.
cogs_lambda = aws.lambda_.Function(
    "shopify-cogs",
    code=pulumi.AssetArchive({".": pulumi.FileArchive("./src/shopify_lambda")}),
    handler="handler_cogs.main",
    runtime="python3.11",
    role=role.arn,
    timeout=900,
    memory_size=1024,
    environment={"variables": {
        "SHOPIFY_ACCESS_TOKEN": token,
        "SHOPIFY_CLIENT_ID": client_id,
        "S3_BUCKET_NAME": BUCKET_NAME,
        "SHOP_NAME": "how2go",
    }},
)

# --- 5. ORDERS TRIGGER: 03:00 UTC daily ---
orders_rule = aws.cloudwatch.EventRule(
    "shopify-daily",
    schedule_expression=ORDERS_RATE,
    description="Fires the Shopify orders Lambda at 03:00 UTC daily.",
)

aws.cloudwatch.EventTarget(
    "daily-target",
    rule=orders_rule.name,
    arn=orders_lambda.arn,
)

aws.lambda_.Permission(
    "daily-perm",
    action="lambda:InvokeFunction",
    function=orders_lambda.name,
    principal="events.amazonaws.com",
    source_arn=orders_rule.arn,
)

# --- 6. COGS TRIGGER: 05:00 UTC daily ---
cogs_rule = aws.cloudwatch.EventRule(
    "shopify-cogs-daily",
    schedule_expression=COGS_RATE,
    description="Fires the Shopify COGS snapshot Lambda at 05:00 UTC daily.",
)

aws.cloudwatch.EventTarget(
    "cogs-target",
    rule=cogs_rule.name,
    arn=cogs_lambda.arn,
)

aws.lambda_.Permission(
    "cogs-perm",
    action="lambda:InvokeFunction",
    function=cogs_lambda.name,
    principal="events.amazonaws.com",
    source_arn=cogs_rule.arn,
)

pulumi.export("orders_lambda_arn", orders_lambda.arn)
pulumi.export("cogs_lambda_arn", cogs_lambda.arn)
pulumi.export("orders_rule_name", orders_rule.name)
pulumi.export("cogs_rule_name", cogs_rule.name)