from aws_cdk import (
    aws_secretsmanager as secretsmanager,
    aws_iam as iam,
    aws_ec2 as ec2,
    aws_lambda as lambda_,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as targets,
    aws_ecr_assets as ecr_assets,
    aws_apigateway as apigateway,
    RemovalPolicy,
    Stack,
    Duration
)
from aws_cdk.aws_lambda_python_alpha import PythonFunction
from aws_cdk.aws_s3 import Bucket
from constructs import Construct


class MyServerlessStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        vpc = ec2.Vpc(self, "irVPC", max_azs=2)

        instance_sg = ec2.SecurityGroup(
            self,
            "InstanceSecurityGroup",
            vpc=vpc,
            description="Allow inbound HTTP traffic on port 8080",
            allow_all_outbound=True
        )
        instance_sg.add_ingress_rule(
            ec2.Peer.any_ipv4(),
            ec2.Port.tcp(8080),
            "Allow inbound HTTP access on port 8080"
        )
        instance_sg.add_ingress_rule(
            ec2.Peer.any_ipv4(),  # Replace with your IP or a trusted CIDR range
            ec2.Port.tcp(22),
            "Allow SSH access"
        )

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

        artifact_bucket: Bucket = Bucket(
            self,
            "ArtifactBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        test_artifact_bucket: Bucket = Bucket(
            self,
            "TestArtifactBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

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
        config_table = dynamodb.Table(
            self,
            "ConfigTable",
            partition_key=dynamodb.Attribute(name="ticker", type=dynamodb.AttributeType.STRING),
            removal_policy=RemovalPolicy.DESTROY
        )
        messages_table = dynamodb.Table(
            self,
            "MessagesTable",
            partition_key=dynamodb.Attribute(name="message_id", type=dynamodb.AttributeType.STRING),
            removal_policy=RemovalPolicy.DESTROY
        )

        worker_image_asset = ecr_assets.DockerImageAsset(
            self,
            "WorkerImageAsset",
            directory="../services/worker",
        )

        ec2_instance_role = iam.Role(
            self,
            "EC2InstanceRole",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonEC2ContainerRegistryReadOnly")
            ]
        )

        ec2_instance_role.add_to_policy(
            iam.PolicyStatement(
                actions=["ec2:TerminateInstances"],
                resources=["*"],  # or scope it to the instance ARN if desired
            )
        )

        groq_api_secret.grant_read(ec2_instance_role)
        discord_webhook_url.grant_read(ec2_instance_role)
        artifact_bucket.grant_put(ec2_instance_role)
        messages_table.grant_write_data(ec2_instance_role)

        instance_profile = iam.CfnInstanceProfile(
            self,
            "InstanceProfile",
            roles=[ec2_instance_role.role_name]
        )

        manager_function = PythonFunction(
            self,
            "ManagerFunction",
            entry="../serverless/manager",
            index="manager.py",
            handler="lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            timeout=Duration.seconds(60 * 15),
            environment={
                "TABLE_NAME": scheduling_table.table_name,
                "WORKER_IMAGE_URI": worker_image_asset.image_uri,
                "HISTORICAL_TABLE": historical_table.table_name,
                "CONFIG_TABLE": config_table.table_name,
                "MESSAGES_TABLE": messages_table.table_name,
                "AWS_ACCOUNT_ID": self.account,
                "GROQ_API_SECRET_ARN": groq_api_secret.secret_arn, 
                "DISCORD_WEBHOOK_SECRET_ARN": discord_webhook_url.secret_arn,
                "INSTANCE_PROFILE": instance_profile.ref,
                "SUBNET_ID": vpc.public_subnets[0].subnet_id,
                "INSTANCE_SECURITY_GROUP": instance_sg.security_group_id,
                "ARTIFACT_BUCKET": artifact_bucket.bucket_name,
            },
        )

        # Grant manager function read access to the scheduling table
        scheduling_table.grant_read_data(manager_function)
        historical_table.grant_read_data(manager_function)
        config_table.grant_read_data(manager_function)

        manager_function.role.add_to_principal_policy(
            iam.PolicyStatement(
                actions=[
                    "ec2:DescribeInstances",
                    "ec2:StartInstances",
                    "ec2:StopInstances",
                    "ec2:TerminateInstances",
                    "ec2:RebootInstances",
                    "ec2:RunInstances",
                    "ec2:CreateTags",
                    "ec2:ModifyInstanceAttribute"
                ],
                resources=["*"],
            )
        )

        manager_function.role.add_to_principal_policy(
            iam.PolicyStatement(
                actions=[
                    "events:PutRule",
                    "events:PutTargets",
                    "events:DeleteRule",
                    "events:RemoveTargets",
                ],
                resources=["*"],
            )
        )

        manager_function.role.add_to_principal_policy(
            iam.PolicyStatement(
                actions=["iam:PassRole"],
                resources=[f"arn:aws:iam::{self.account}:role/EventBridgeInvokeLambdaRole"],
            )
        )

        manager_function.role.add_to_principal_policy(
            iam.PolicyStatement(
                actions=["iam:PassRole"],
                resources=[ec2_instance_role.role_arn],
            )
        )

        manager_function.role.add_to_principal_policy(
            iam.PolicyStatement(
                actions=["dynamodb:Query"],
                resources=[f"{scheduling_table.table_arn}/index/date-index"],
            )
        )

        before_market_rule = events.Rule(
            self,
            "BeforeMarketStartRule",
            schedule=events.Schedule.cron(
                minute="55",
                hour="10",
                month="*",
                week_day="MON-FRI",
                year="*"
            )
        )
        before_market_rule.add_target(targets.LambdaFunction(manager_function, event=events.RuleTargetInput.from_object({
            "release_time": "before"
        })))

        after_market_rule = events.Rule(
            self,
            "AfterMarketStartRule",
            schedule=events.Schedule.cron(
                minute="55",
                hour="20",
                month="*",
                week_day="MON-FRI",
                year="*"
            )
        )
        after_market_rule.add_target(targets.LambdaFunction(manager_function, event=events.RuleTargetInput.from_object({
            "release_time": "after"
        })))

        scheduler_function = PythonFunction(
            self, 
            "Scheduler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            entry="../serverless/scheduler",
            index="scheduler.py",
            handler="lambda_handler",
            timeout=Duration.minutes(3),
            environment={ "TABLE_NAME": scheduling_table.table_name }
        )
        scheduling_table.grant_write_data(scheduler_function)

        schedule_rule = events.Rule(
            self,
            "EarningsScheduleExtractionRule",
            schedule=events.Schedule.cron(
                minute="0",
                hour="0",
                month="*",
                week_day="MON-FRI",
                year="*"
            )
        )
        schedule_rule.add_target(targets.LambdaFunction(scheduler_function))

        schedule_handler = PythonFunction(
            self, "ScheduleHandler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            entry="../serverless/database_handlers/schedule",
            index="schedule.py",
            handler="handler",
            environment={"SCHEDULE_TABLE": scheduling_table.table_name}
        )
        scheduling_table.grant_read_data(schedule_handler)
        scheduling_table.grant_write_data(schedule_handler)

        history_handler = PythonFunction(
            self, "HistoryHandler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            entry="../serverless/database_handlers/history",
            index="history.py",
            handler="handler",
            environment={"HISTORY_TABLE": historical_table.table_name}
        )
        historical_table.grant_read_data(history_handler)
        historical_table.grant_write_data(history_handler)

        config_handler = PythonFunction(
            self, "ConfigHandler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            entry="../serverless/database_handlers/config",
            index="config.py",
            handler="handler",
            environment={"CONFIG_TABLE": config_table.table_name}
        )
        config_table.grant_read_data(config_handler)
        config_table.grant_write_data(config_handler)

        message_handler = PythonFunction(
            self,
            "MessageHandler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            entry="../serverless/database_handlers/messages",  # Folder containing handler.py
            index="messages.py",
            handler="handler",
            environment={
                "MESSAGES_TABLE": messages_table.table_name,
            },
        )
        messages_table.grant_read_data(message_handler)
        messages_table.grant_write_data(message_handler)

        # Earnings API Gateway
        earnings_api = apigateway.LambdaRestApi(
            self, "EarningsAPI",
            handler=schedule_handler,
            proxy=False,
            default_cors_preflight_options=apigateway.CorsOptions(
                allow_origins=apigateway.Cors.ALL_ORIGINS,
                allow_methods=apigateway.Cors.ALL_METHODS,
            )
        )
        earnings_resource = earnings_api.root.add_resource("earnings")
        earnings_resource.add_method("GET", api_key_required=True)
        earnings_resource.add_method("POST", api_key_required=True)
        earnings_resource.add_method("PUT", api_key_required=True)

        earnings_api_key = earnings_api.add_api_key("EarningsApiKey", api_key_name="EarningsApiKey")
        earnings_usage_plan = earnings_api.add_usage_plan(
            "EarningsUsagePlan",
            name="EarningsUsagePlan",
            throttle=apigateway.ThrottleSettings(rate_limit=10, burst_limit=2),
            api_stages=[apigateway.UsagePlanPerApiStage(
                api=earnings_api,
                stage=earnings_api.deployment_stage
            )]
        )
        earnings_usage_plan.add_api_key(earnings_api_key)

        # Historical API Gateway
        historical_api = apigateway.LambdaRestApi(
            self, "HistoricalAPI",
            handler=history_handler,
            proxy=False,
            default_cors_preflight_options=apigateway.CorsOptions(
                allow_origins=apigateway.Cors.ALL_ORIGINS,
                allow_methods=apigateway.Cors.ALL_METHODS,
            )
        )
        historical_resource = historical_api.root.add_resource("historical")
        historical_resource.add_method("GET", api_key_required=True)
        historical_resource.add_method("POST", api_key_required=True)
        ticker_resource = historical_resource.add_resource("{ticker}")
        ticker_resource.add_resource("{date}").add_method("GET", api_key_required=True)

        historical_api_key = historical_api.add_api_key("HistoricalApiKey", api_key_name="HistoricalApiKey")
        historical_usage_plan = historical_api.add_usage_plan(
            "HistoricalUsagePlan",
            name="HistoricalUsagePlan",
            throttle=apigateway.ThrottleSettings(rate_limit=10, burst_limit=2),
            api_stages=[apigateway.UsagePlanPerApiStage(
                api=historical_api,
                stage=historical_api.deployment_stage
            )]
        )
        historical_usage_plan.add_api_key(historical_api_key)

        company_api = apigateway.LambdaRestApi(
            self, "CompanyAPI",
            handler=config_handler,
            proxy=False,
            default_cors_preflight_options=apigateway.CorsOptions(
                allow_origins=apigateway.Cors.ALL_ORIGINS,
                allow_methods=apigateway.Cors.ALL_METHODS,
            )
        )
        configs_resource = company_api.root.add_resource("configs")
        configs_resource.add_method("GET", api_key_required=True)
        configs_resource.add_method("POST", api_key_required=True)
        configs_resource.add_resource("{ticker}").add_method("GET", api_key_required=True)

        company_api_key = company_api.add_api_key("CompanyApiKey", api_key_name="CompanyApiKey")
        company_usage_plan = company_api.add_usage_plan(
            "CompanyUsagePlan",
            name="CompanyUsagePlan",
            throttle=apigateway.ThrottleSettings(rate_limit=10, burst_limit=2),
            api_stages=[apigateway.UsagePlanPerApiStage(
                api=company_api,
                stage=company_api.deployment_stage
            )]
        )
        company_usage_plan.add_api_key(company_api_key)

        # API Gateway with CORS and API Key configuration
        messages_api: apigateway.LambdaRestApi = apigateway.LambdaRestApi(
            self,
            "MessagesAPI",
            handler=message_handler,
            proxy=False,
            default_cors_preflight_options=apigateway.CorsOptions(
                allow_origins=apigateway.Cors.ALL_ORIGINS,
                allow_methods=apigateway.Cors.ALL_METHODS,
            ),
        )

        messages_resource = messages_api.root.add_resource("messages")
        messages_resource.add_method("GET", api_key_required=True)
        messages_resource.add_method("POST", api_key_required=True)

        message_by_id = messages_resource.add_resource("{id}")
        message_by_id.add_method("GET", api_key_required=True)
        message_by_id.add_method("DELETE", api_key_required=True)

        read_resource = message_by_id.add_resource("read")
        read_resource.add_method("PATCH", api_key_required=True)

        messages_api_key = messages_api.add_api_key(
            "MessagesApiKey", api_key_name="MessagesApiKey"
        )
        messages_usage_plan = messages_api.add_usage_plan(
            "MessagesUsagePlan",
            name="MessagesUsagePlan",
            throttle=apigateway.ThrottleSettings(rate_limit=10, burst_limit=2),
            api_stages=[
                apigateway.UsagePlanPerApiStage(
                    api=messages_api, stage=messages_api.deployment_stage
                )
            ],
        )
        messages_usage_plan.add_api_key(messages_api_key)