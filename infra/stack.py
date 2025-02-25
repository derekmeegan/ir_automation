from aws_cdk import (
    aws_secretsmanager as secretsmanager,
    aws_iam as iam,
    aws_ec2 as ec2,
    aws_lambda as lambda_,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as targets,
    aws_ecr_assets as ecr_assets,
    RemovalPolicy,
    Stack,
    Duration
)
from aws_cdk.aws_lambda_python_alpha import PythonFunction
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

        worker_lambda_execution_role = iam.Role(
            self,
            "WorkerLambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
        )

        worker_lambda_execution_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
        )

        groq_api_secret.grant_read(worker_lambda_execution_role)
        discord_webhook_url.grant_read(worker_lambda_execution_role)

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

        worker_image_asset = ecr_assets.DockerImageAsset(
            self,
            "WorkerImageAsset",
            directory="../serverless/worker",
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
                "WORKER_EXECUTION_ROLE": worker_lambda_execution_role.role_arn,
                "HISTORICAL_TABLE": historical_table.table_name,
                "CONFIG_TABLE": config_table.table_name,
                "AWS_ACCOUNT_ID": self.account,
                "GROQ_API_SECRET_ARN": groq_api_secret.secret_arn, 
                "DISCORD_WEBHOOK_SECRET_ARN": discord_webhook_url.secret_arn,
                "INSTANCE_PROFILE": instance_profile.ref,
                "SUBNET_ID": vpc.public_subnets[0].subnet_id,
                "INSTANCE_SECURITY_GROUP": instance_sg.security_group_id,
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
                ],
                resources=["*"],
            )
        )

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
                resources=[worker_lambda_execution_role.role_arn],
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
                minute="50",
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
                minute="50",
                hour="19",
                month="*",
                week_day="MON-FRI",
                year="*"
            )
        )
        after_market_rule.add_target(targets.LambdaFunction(manager_function, event=events.RuleTargetInput.from_object({
            "release_time": "after"
        })))

        # pandas_layer = lambda_.LayerVersion(
        #     self, "PandasLayer",
        #     code=lambda_.Code.from_asset("../serverless/scheduler/lambda_layer"),
        #     compatible_runtimes=[lambda_.Runtime.PYTHON_3_9],
        #     description="Layer with Pandas library"
        # )

        scheduler_function = PythonFunction(
            self, 
            "Scheduler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            entry="../serverless/scheduler",
            index="scheduler.py",
            handler="lambda_handler",
            # layers=[pandas_layer],
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


