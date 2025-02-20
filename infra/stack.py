from aws_cdk import (
    aws_secretsmanager as secretsmanager,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
    aws_events as events,
    aws_events_targets as targets,
    aws_ecr_assets as ecr_assets,
    RemovalPolicy,
    Stack,
)
from aws_cdk.aws_lambda_python_alpha import PythonFunction
from constructs import Construct


class MyServerlessStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        groq_api_secret = secretsmanager.Secret.from_secret_name_v2(
            self,
            "GroqApiKeySecret",
            "IRWorkflow/GroqApiKey"
        )
        
        discord_webhook_url = secretsmanager.Secret.from_secret_name_v2(
            self,
            "DiscordWebhookURL",
            "IRWorkflow/DiscordWebhookAPI"
        )

        worker_lambda_execution_role = iam.Role(
            self,
            "WorkerLambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
        )

        # Attach at least the basic execution policy for Lambda logs, etc.
        worker_lambda_execution_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
        )

        # Grant worker Lambda permission to access the secrets
        groq_api_secret.grant_read(worker_lambda_execution_role)
        discord_webhook_url.grant_read(worker_lambda_execution_role)

        # 1) DynamoDB scheduling table
        scheduling_table = dynamodb.Table(
            self,
            "SchedulingTable",
            partition_key=dynamodb.Attribute(name="date", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="ticker", type=dynamodb.AttributeType.STRING),
            removal_policy=RemovalPolicy.DESTROY
        )

        historical_table = dynamodb.Table(
            self,
            "HistoricalTable",
            partition_key=dynamodb.Attribute(name="ticker", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="date", type=dynamodb.AttributeType.STRING),
            removal_policy=RemovalPolicy.DESTROY
        )
        historical_table.add_global_secondary_index(
            index_name="date-index",
            partition_key=dynamodb.Attribute(name="ticker", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="date", type=dynamodb.AttributeType.STRING)
        )

        # Config Table (assumes partition key is "ticker")
        config_table = dynamodb.Table(
            self,
            "ConfigTable",
            partition_key=dynamodb.Attribute(name="ticker", type=dynamodb.AttributeType.STRING),
            removal_policy=RemovalPolicy.DESTROY
        )

        # 2) Docker image for the WORKER function
        #    This pushes an image to ECR at deploy time.
        worker_image_asset = ecr_assets.DockerImageAsset(
            self,
            "WorkerImageAsset",
            directory="../serverless/worker",  # Path to your Dockerfile and code
        )

        disabler_function = PythonFunction(
            self,
            "DisablerFunction",
            entry="../serverless/disabler",
            index="disabler.py",
            handler="lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
        )

        disabler_function_url = disabler_function.add_function_url(
            auth_type=lambda_.FunctionUrlAuthType.NONE
        )

        # 3) Manager function (standard Python ZIP)
        #    This function will read from the scheduling table and create new worker Lambdas.
        manager_function = PythonFunction(
            self,
            "ManagerFunction",
            entry="../serverless/manager",
            index="manager.py",
            handler="lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            environment={
                "TABLE_NAME": scheduling_table.table_name,
                "WORKER_IMAGE_URI": worker_image_asset.image_uri,
                "WORKER_EXECUTION_ROLE": worker_lambda_execution_role.role_arn,
                "HISTORICAL_TABLE": historical_table.table_name,
                "CONFIG_TABLE": config_table.table_name,
                "AWS_ACCOUNT_ID": self.account,
                "GROQ_API_SECRET_ARN": groq_api_secret.secret_arn, 
                "DISCORD_WEBHOOK_SECRET_ARN": discord_webhook_url.secret_arn,
                "DISABLER_URL": disabler_function_url.url,
            },
        )

        # Grant manager function read access to the scheduling table
        scheduling_table.grant_read_data(manager_function)
        historical_table.grant_read_data(manager_function)
        config_table.grant_read_data(manager_function)

        # Grant manager function permission to create/update worker Lambdas
        manager_function.role.add_to_principal_policy(
            iam.PolicyStatement(
                actions=[
                    "lambda:CreateFunction",
                    "lambda:UpdateFunctionConfiguration",
                    "lambda:UpdateFunctionCode",
                    "lambda:GetFunction",
                    # optionally "lambda:InvokeFunction" if needed
                ],
                resources=["*"],
            )
        )

        # Permissions to manipulate EventBridge
        manager_function.role.add_to_principal_policy(
            iam.PolicyStatement(
                actions=[
                    "events:PutRule",
                    "events:PutTargets",
                    "events:DeleteRule",
                    "events:RemoveTargets",
                    # ...
                ],
                resources=["*"],
            )
        )

        manager_function.role.add_to_principal_policy(
            iam.PolicyStatement(
                actions=["iam:PassRole"],
                resources=[worker_lambda_execution_role.role_arn],
            )
        )

        manager_function.role.add_to_principal_policy(
            iam.PolicyStatement(
                actions=["dynamodb:Query"],
                resources=[f"{scheduling_table.table_arn}/index/date-index"],
            )
        )

        # 4) EventBridge rule to trigger manager function daily at 00:00 UTC
        daily_rule = events.Rule(
            self,
            "DailyManagerTrigger",
            schedule=events.Schedule.cron(minute="0", hour="0")
        )
        daily_rule.add_target(targets.LambdaFunction(manager_function))
