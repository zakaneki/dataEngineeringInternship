from aws_cdk import aws_lambda, aws_ecr_assets
from aws_cdk import (
    App,
    Stack,
    CfnOutput
)

from constructs import Construct  # CDK v2 uses constructs for defining stacks

class LambdaLayerImageCdkStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Lambda Layer
        layer = aws_lambda.LayerVersion(
            self, "MyLambdaLayerAZ",
            code=aws_lambda.Code.from_asset("layer/layer.zip"),
            compatible_runtimes=[aws_lambda.Runtime.PYTHON_3_13],
            description="A sample Lambda layer with dependencies."
        )

        # 2. Create the Docker Image Lambda
        docker_image_function = aws_lambda.DockerImageFunction(
            self, "MyDockerLambdaAZ",
            code=aws_lambda.DockerImageCode.from_image_asset(
                directory="lambda-image"  # Path to the Dockerfile
            )
        )

        self.layer_arn = layer.layer_version_arn
        self.lambda_function_name = docker_image_function.function_name
