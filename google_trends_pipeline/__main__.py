import json
import pulumi
import pulumi_aws as aws

# -------------------------------
# Constants
# -------------------------------
S3_BUCKET_NAME = "sources-data"

SNOWFLAKE_SECRET_NAME = "snowflake_private_key"   # Secrets Manager secret name
SERPAPI_SECRET_NAME = "serpapi_key"               # Secrets Manager secret name

# Put your brands here (or read from a file/db later)
# Dispatcher sends each brand as one SQS message.
BRANDS = [
    {"BRAND": "Huel", "ENTITY_ID": None},
    {"BRAND": "Athletic Greens", "ENTITY_ID": None},
    # Add the rest...
]

# -------------------------------
# 1) IAM Role for Lambdas
# -------------------------------
lambda_role = aws.iam.Role(
    "google-trends-lambda-role",
    assume_role_policy=json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Action": "sts:AssumeRole",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Effect": "Allow",
        }],
    }),
)

aws.iam.RolePolicyAttachment(
    "lambda-logs",
    role=lambda_role.name,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
)

# You used this already; leaving as-is for simplicity (broad but works)
aws.iam.RolePolicyAttachment(
    "lambda-sqs",
    role=lambda_role.name,
    policy_arn="arn:aws:iam::aws:policy/AmazonSQSFullAccess",
)

# Extra permissions: Secrets Manager + S3 writes
aws.iam.RolePolicy(
    "lambda-extra-permissions",
    role=lambda_role.id,
    policy=json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            # Read secrets (Snowflake key + SerpAPI key)
            {
                "Effect": "Allow",
                "Action": ["secretsmanager:GetSecretValue"],
                "Resource": [
                    f"arn:aws:secretsmanager:*:*:secret:{SNOWFLAKE_SECRET_NAME}*",
                    f"arn:aws:secretsmanager:*:*:secret:{SERPAPI_SECRET_NAME}*",
                ],
            },
            # Write outputs to S3
            {
                "Effect": "Allow",
                "Action": ["s3:PutObject"],
                "Resource": [f"arn:aws:s3:::{S3_BUCKET_NAME}/google_trends/*"],
            },
            # Optional list (not required for PutObject)
            {
                "Effect": "Allow",
                "Action": ["s3:ListBucket"],
                "Resource": [f"arn:aws:s3:::{S3_BUCKET_NAME}"],
                "Condition": {"StringLike": {"s3:prefix": ["google_trends/*"]}},
            },
        ],
    }),
)

# -------------------------------
# 2) SQS Queue
# -------------------------------
trends_queue = aws.sqs.Queue(
    "google-trends-queue",
    visibility_timeout_seconds=900,
)

# -------------------------------
# 3) Dispatcher Lambda
# -------------------------------
dispatcher_lambda = aws.lambda_.Function(
    "trends-dispatcher",
    role=lambda_role.arn,
    runtime="python3.11",
    handler="dispatcher.handler",
    code=pulumi.AssetArchive({
        ".": pulumi.FileArchive("./lambdas/dispatcher")
    }),
    timeout=60,
    environment=aws.lambda_.FunctionEnvironmentArgs(
        variables={
            "SQS_QUEUE_URL": trends_queue.id,
            "BRANDS_JSON": json.dumps(BRANDS),
        }
    ),
)

# -------------------------------
# 4) Worker Lambda
# -------------------------------
worker_lambda = aws.lambda_.Function(
    "trends-worker",
    role=lambda_role.arn,
    runtime="python3.11",
    handler="worker.handler",
    code=pulumi.AssetArchive({
        ".": pulumi.FileArchive("./lambdas/worker")
    }),
    timeout=900,
    memory_size=1024,
    environment=aws.lambda_.FunctionEnvironmentArgs(
        variables={
            # Secrets Manager IDs (names)
            "SERPAPI_SECRET_ID": SERPAPI_SECRET_NAME,
            "SNOWFLAKE_PRIVATE_KEY_SECRET_ID": SNOWFLAKE_SECRET_NAME,

            # Snowflake connection (non-secret)
            "SNOWFLAKE_USER": "SJ_SERVICE_USER",
            "SNOWFLAKE_ACCOUNT": "GWNDCGK-GN77379",
            "SNOWFLAKE_WAREHOUSE": "HEALF_WH",
            "SNOWFLAKE_DATABASE": "HEALF",
            "SNOWFLAKE_SCHEMA": "GOOGLE_ADS",
            "SNOWFLAKE_ROLE": "PC_THOUGHTSPOT_ROLE",

            # S3 output
            "S3_BUCKET": S3_BUCKET_NAME,
        }
    ),
)

# -------------------------------
# 5) Connect SQS to Worker
# -------------------------------
aws.lambda_.EventSourceMapping(
    "sqs-to-worker",
    event_source_arn=trends_queue.arn,
    function_name=worker_lambda.name,
    batch_size=10,  # better than 1; one invocation handles multiple brands
)

# -------------------------------
# 6) Weekly Trigger (Mondays 08:00 UTC)
# -------------------------------
monday_trigger = aws.cloudwatch.EventRule(
    "monday-8am-trigger",
    schedule_expression="cron(0 8 ? * MON *)",
)

aws.cloudwatch.EventTarget(
    "trigger-dispatcher",
    rule=monday_trigger.name,
    arn=dispatcher_lambda.arn,
)

aws.lambda_.Permission(
    "allow-eventbridge",
    action="lambda:InvokeFunction",
    function=dispatcher_lambda.name,
    principal="events.amazonaws.com",
    source_arn=monday_trigger.arn,
)

pulumi.export("dispatcher_name", dispatcher_lambda.name)
pulumi.export("worker_name", worker_lambda.name)
pulumi.export("queue_url", trends_queue.id)