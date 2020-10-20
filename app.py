#!/usr/bin/env python3

from aws_cdk import core

from parallelagram.parallelagram_stack import ParallelagramStack


app = core.App()
ParallelagramStack(app, "parallelagram", env={'region': 'us-west-2'})

app.synth()
