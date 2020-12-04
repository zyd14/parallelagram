import setuptools


with open("README.md") as fp:
    long_description = fp.read()


setuptools.setup(
    name="parallelagram",
    version="0.0.1",

    description="A Python library providing tools for distributing parallelizable workloads across AWS compute services",
    long_description=long_description,
    long_description_content_type="text/markdown",

    author="zyd14",

    package_dir={"": "parallelagram"},
    packages=setuptools.find_packages(where="parallelagram"),

    install_requires=[
        "aws-cdk.core==1.71.0",
        "aws-cdk.aws_iam==1.71.0",
        "aws-cdk.aws_sqs==1.71.0",
        "aws-cdk.aws_sns==1.71.0",
        "aws-cdk.aws_sns_subscriptions==1.71.0",
        "aws-cdk.aws_s3==1.71.0",
        "boto3"
    ],

    python_requires=">=3.6",

    classifiers=[
        "Development Status :: 4 - Beta",

        "Intended Audience :: Developers",

        "License :: OSI Approved :: Apache Software License",

        "Programming Language :: JavaScript",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",

        "Topic :: Software Development :: Code Generators",
        "Topic :: Utilities",

        "Typing :: Typed",
    ],
)
