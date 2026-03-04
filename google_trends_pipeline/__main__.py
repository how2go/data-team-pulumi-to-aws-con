import pulumi
import pulumi_aws as aws
import json

# 1. IAM Role for Lambda Execution
# This role allows our functions to run, log to CloudWatch, and interact with SQS
lambda_role = aws.iam.Role("google-trends-lambda-role",
    assume_role_policy=json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Action": "sts:AssumeRole",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Effect": "Allow",
            "Sid": "",
        }]
    }))

# Attach standard logging and SQS permissions
aws.iam.RolePolicyAttachment("lambda-logs",
    role=lambda_role.name,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole")

aws.iam.RolePolicyAttachment("lambda-sqs",
    role=lambda_role.name,
    policy_arn="arn:aws:iam::aws:policy/AmazonSQSFullAccess")

# 2. SQS Queue
# Bridge between Dispatcher and Worker. 15-minute visibility timeout.
trends_queue = aws.sqs.Queue("google-trends-queue",
    visibility_timeout_seconds=900)

# 3. Dispatcher Lambda (The Postman)
# handler="dispatcher.handler" tells AWS to look for handler() in dispatcher.py
dispatcher_lambda = aws.lambda_.Function("trends-dispatcher",
    role=lambda_role.arn,
    runtime="python3.11",
    handler="dispatcher.handler",
    code=pulumi.AssetArchive({
        ".": pulumi.FileArchive("./lambdas/dispatcher")
    }),
    timeout=60,
    environment={
        "variables": {
            "SQS_QUEUE_URL": trends_queue.id,
            "SNOWFLAKE_USER": "SJ_SERVICE_USER",
            "SNOWFLAKE_ACCOUNT": "GWNDCGK-GN77379",
            "SNOWFLAKE_WAREHOUSE": "HEALF_WH",
            "SNOWFLAKE_PRIVATE_KEY": pulumi.Config().require_secret("snowflake_private_key"),
        }
    })

# 4. Worker Lambda (The Engine)
# handler="worker.handler" tells AWS to look for handler() in worker.py
worker_lambda = aws.lambda_.Function("trends-worker",
    role=lambda_role.arn,
    runtime="python3.11",
    handler="worker.handler",
    code=pulumi.AssetArchive({
        ".": pulumi.FileArchive("./lambdas/worker")
    }),
    timeout=900,
    memory_size=512,
    environment={
        "variables": {
            "SERPAPI_KEY": pulumi.Config().require_secret("serpapi_key"),
            "SNOWFLAKE_USER": "SJ_SERVICE_USER",
            "SNOWFLAKE_ACCOUNT": "GWNDCGK-GN77379",
            "SNOWFLAKE_WAREHOUSE": "HEALF_WH",
            "SNOWFLAKE_PRIVATE_KEY": pulumi.Config().require_secret("snowflake_private_key"),
            "SNOWFLAKE_DATABASE": "HEALF",
            "SNOWFLAKE_SCHEMA": "GOOGLE_ADS"
        }
    })

# 5. Connect SQS to the Worker
aws.lambda_.EventSourceMapping("sqs-to-worker",
    event_source_arn=trends_queue.arn,
    function_name=worker_lambda.name,
    batch_size=1)

# 6. Weekly Schedule Trigger (Mondays at 8 AM UTC)
monday_trigger = aws.cloudwatch.EventRule("monday-8am-trigger",
    schedule_expression="cron(0 8 ? * MON *)")

aws.cloudwatch.EventTarget("trigger-dispatcher",
    rule=monday_trigger.name,
    arn=dispatcher_lambda.arn)

aws.lambda_.Permission("allow-eventbridge",
    action="lambda:InvokeFunction",
    function=dispatcher_lambda.name,
    principal="events.amazonaws.com",
    source_arn=monday_trigger.arn)

# Export names for manual invocation
pulumi.export("dispatcher_name", dispatcher_lambda.name)