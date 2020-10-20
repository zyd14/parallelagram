#!/usr/bin/env python3

from aws_cdk import core

from parallelagram.parallelagram_stack import LambdaStack


app = core.App()
LambdaStack(app, "parallelagram", env={'region': 'us-west-2'})

app.synth()
