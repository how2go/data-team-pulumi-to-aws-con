import json
import pulumi
import pulumi_aws as aws

# -------------------------------
# Constants
# -------------------------------
S3_BUCKET_NAME = "sources-data"

SNOWFLAKE_SECRET_NAME = "snowflake_private_key"  # Secrets Manager secret name
SERPAPI_SECRET_NAME = "serpapi_key"              # Secrets Manager secret name

# -------------------------------
# 1) IAM Role for Lambda Execution
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

# CloudWatch logs
aws.iam.RolePolicyAttachment(
    "lambda-logs",
    role=lambda_role.name,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
)

# SQS access (broad but simple)
aws.iam.RolePolicyAttachment(
    "lambda-sqs",
    role=lambda_role.name,
    policy_arn="arn:aws:iam::aws:policy/AmazonSQSFullAccess",
)

# Secrets Manager + S3 permissions
aws.iam.RolePolicy(
    "lambda-extra-permissions",
    role=lambda_role.id,
    policy=json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            # Read both secrets (Snowflake key + SerpAPI key)
            {
                "Effect": "Allow",
                "Action": ["secretsmanager:GetSecretValue"],
                "Resource": [
                    f"arn:aws:secretsmanager:*:*:secret:{SNOWFLAKE_SECRET_NAME}*",
                    f"arn:aws:secretsmanager:*:*:secret:{SERPAPI_SECRET_NAME}*",
                ],
            },
            # Write outputs to S3 prefix
            {
                "Effect": "Allow",
                "Action": ["s3:PutObject"],
                "Resource": [f"arn:aws:s3:::{S3_BUCKET_NAME}/google_trends/*"],
            },
            # Optional list prefix
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
# 2) SQS Queue (Dispatcher -> Worker)
# -------------------------------
trends_queue = aws.sqs.Queue(
    "google-trends-queue",
    visibility_timeout_seconds=900,
)

# -------------------------------
# 3) Lambda Layers
# -------------------------------
# Dispatcher layer: contains snowflake-connector-python + cryptography
dispatcher_deps_layer = aws.lambda_.LayerVersion(
    "dispatcher-deps-layer",
    layer_name="google-trends-dispatcher-deps",
    compatible_runtimes=["python3.11"],
    code=pulumi.FileArchive("./lambda_layer_dispatcher"),
)

# Worker layer: contains pandas/numpy/requests/snowflake/cryptography (you will build this)
worker_deps_layer = aws.lambda_.LayerVersion(
    "worker-deps-layer",
    layer_name="google-trends-worker-deps",
    compatible_runtimes=["python3.11"],
    code=pulumi.FileArchive("./lambda_layer_worker"),
)

# -------------------------------
# 4) Dispatcher Lambda
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
    layers=[dispatcher_deps_layer.arn],
    environment=aws.lambda_.FunctionEnvironmentArgs(
        variables={
            "SQS_QUEUE_URL": trends_queue.id,

            # Snowflake (non-secret settings)
            "SNOWFLAKE_USER": "SJ_SERVICE_USER",
            "SNOWFLAKE_ACCOUNT": "GWNDCGK-GN77379",
            "SNOWFLAKE_WAREHOUSE": "HEALF_WH",

            # Secret name for Snowflake key
            "SNOWFLAKE_PRIVATE_KEY_SECRET_ID": SNOWFLAKE_SECRET_NAME,
        }
    ),
)

# -------------------------------
# 5) Worker Lambda
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
    layers=[worker_deps_layer.arn],
    environment=aws.lambda_.FunctionEnvironmentArgs(
        variables={
            # Secrets Manager secret names
            "SERPAPI_SECRET_ID": SERPAPI_SECRET_NAME,
            "SNOWFLAKE_PRIVATE_KEY_SECRET_ID": SNOWFLAKE_SECRET_NAME,

            # Snowflake (non-secret)
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
# 6) Connect SQS -> Worker
# -------------------------------
aws.lambda_.EventSourceMapping(
    "sqs-to-worker",
    event_source_arn=trends_queue.arn,
    function_name=worker_lambda.name,
    batch_size=10,
)

# -------------------------------
# 7) Weekly Trigger (Mondays 08:00 UTC)
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

# -------------------------------
# Outputs
# -------------------------------
pulumi.export("dispatcher_name", dispatcher_lambda.name)
pulumi.export("worker_name", worker_lambda.name)
pulumi.export("queue_url", trends_queue.id)