# Parallelagram
A tool for distributing processes across AWS Lambda functions with built-in result retrieval for embarrassingly parallel workloads.  

## Background  
While working on projects to build compute infrastructure for biological analyses in AWS I noticed an issue that was setting 
a lower bound on how quickly an analysis would complete - the time it takes to simply provision the resources requested 
to carry out the analysis. My team was using a combination of AWS Batch and Fargate to provision resources, each having
their own advantages and disadvantages.  Batch could take quite some time to provision resources (often 10-20 minutes), 
although once provisioned you could have pretty powerful machines at your disposal. Although sometimes not enough machines 
would be allocated for all the tasks in our queue, and 10 machines would spend 2 hours chugging through a few hundred tasks. 
Fargate provisions very quickly (usually 30-90s), but at a maximum of 1 task per second and with a much more limited CPU / RAM 
configuration. Most of our tasks took anywhere from 1-20 minutes to complete once provisioned, so it was unfortunate that 
often at least half the total task duration was spent provisioning machines. Furthermore, this built-in provisioning latency 
reduced the impact any further parallelization across machines would provide for us.  

Enter AWS Lambda. We had been using Lambda to provide all of our service-layer components for quite some time, and were 
generally pretty pleased with its performance in the context of workflow management and CRUD operations. But what if we 
could break down our tasks into hundreds (or thousands) of pieces, and invoke a Lambda on each piece? We could then reap the 
benefits of the low-latency provisioning time of Lambda, while also effectively launching processes on hundreds of cores in a 
massively parallel fashion. The hurtles of integrating Lambda in a processing pipeline in this fashion are many, and this 
library attempts to tackle some of them in a pure Python package (but please do see the caveats and pitfalls below).  

## Who might want to use this?  
If you have a task which has embarrassingly parallel properties and an AWS account, you may consider using this library. 
It can be launched from a remote EC2 instance, an ECS container, your workstation, or even another Lambda function (really anywhere) 
in order to gain access to hundreds or thousands of CPUs in a few seconds. Please do see the *Caveats and pitfalls* below 
prior to using this library though; using this library can incur account charges by AWS and can also interfere with other 
services in your account by using up account-level quotas.  

## Usage  

## Caveats and pitfalls  
With great power comes great responsibility. When using this library it is important to be aware of some side-effects that 
can occur as a result of provisioning large amounts of concurrent Lambda functions;
### Account limits
    - Concurrent Lambda Invocation quotas: the most obvious quota you may quickly bump up against is the concurrent Lambda invocation quota. When you attempt to provision more concurrent Lambdas than your account settings allow, AWS will queue up the remaining asynchronous invocations for you for up to 6 hours <AWS Lambda async documentation>. This is great, and means that even if you blow your account concurrency limits your Lambdas will still get executed eventually. The downside is that once you hit your concurrency limit any other asynchronous Lambda invocations (message / event - driven) will be queued up behind your parallelagram-provisioned Lambdas, and any synchronous Lambda invocations (direct invocation or API Gateway provisioned) will fail. The solution is to set a reserved concurrency on your Lambda function <insert AWS documentation> less than your total account concurrency, allowing other Lambda functions to execute with the rest of your account-wide concurrency.   
    - CreateLogStream quotas: an interesting side effect of provisioning hundreds or thousands of Lambda instances concurrently is that each one attempts to create its own log stream, which drains on the account-wide CreateLogStream quota. The default CreateLogStream quota is 50/s, but this is another quota AWS can adjust for you on request. When this quota is met you may see failures with other services which attempt to create log streams, such as ECS and Batch. In addition to having AWS increase the default quota, you can also create an IAM role for the parallelagram-provisioned Lambda to use which doesn't have CreateLogStream privileges (although this is obviously not ideal as you won't have any logs from your Lambda invocations).  
    - timeouts 