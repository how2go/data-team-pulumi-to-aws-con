import pulumi
import pulumi_aws as aws
import json

# 1. Configuration & IAM Roles
# We create a basic role that allows Lambdas to log to CloudWatch
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

# Add permissions for CloudWatch Logs and SQS
aws.iam.RolePolicyAttachment("lambda-logs",
    role=lambda_role.name,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole")

aws.iam.RolePolicyAttachment("lambda-sqs",
    role=lambda_role.name,
    policy_arn="arn:aws:iam::aws:policy/AmazonSQSFullAccess")

# 2. Create the SQS Queue
# This is the "bucket" that will hold the 400 brand IDs
trends_queue = aws.sqs.Queue("google-trends-queue",
    visibility_timeout_seconds=900) # Match the 15-minute Lambda limit

# 3. Create the Dispatcher Lambda (The Postman)
dispatcher_lambda = aws.lambda_.Function("trends-dispatcher",
    role=lambda_role.arn,
    runtime="python3.11",
    handler="dispatcher.handler",
    code=pulumi.AssetArchive({
        ".": pulumi.FileArchive("./lambdas/dispatcher")
    }),
    timeout=60, # Runs fast
    environment={
        "variables": {
            "SQS_QUEUE_URL": trends_queue.id,
            "SNOWFLAKE_USER": "SJ_SERVICE_USER",
            "SNOWFLAKE_ACCOUNT": "GWNDCGK-GN77379",
            "SNOWFLAKE_WAREHOUSE": "HEALF_WH",
            "SNOWFLAKE_PRIVATE_KEY": pulumi.Config().require_secret("snowflake_private_key"),
        }
    })

# 4. Create the Worker Lambda (The Engine)
worker_lambda = aws.lambda_.Function("trends-worker",
    role=lambda_role.arn,
    runtime="python3.11",
    handler="worker.handler",
    code=pulumi.AssetArchive({
        ".": pulumi.FileArchive("./lambdas/worker")
    }),
    timeout=900, # 15-minute max
    memory_size=512,
    environment={
        "variables": {
            "SERPAPI_KEY": pulumi.Config().require_secret("serpapi_key"),
            "SNOWFLAKE_USER": "SJ_SERVICE_USER",
            "SNOWFLAKE_ACCOUNT": "GWNDCGK-GN77379",
            "SNOWFLAKE_WAREHOUSE": "HEALF_WH",
            "SNOWFLAKE_PRIVATE_KEY": pulumi.Config().require_secret("snowflake_private_key"),
        }
    })

# 5. Connect SQS to the Worker Lambda
# This tells AWS: "Whenever a message enters the queue, trigger the worker"
aws.lambda_.EventSourceMapping("sqs-to-worker",
    event_source_arn=trends_queue.arn,
    function_name=worker_lambda.name,
    batch_size=1) # One brand per lambda execution for maximum reliability

# 6. Schedule (EventBridge Cron)
# This triggers the Dispatcher every Monday at 8 AM UTC
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

# 7. Outputs
pulumi.export("queue_url", trends_queue.id)
pulumi.export("dispatcher_name", dispatcher_lambda.name)