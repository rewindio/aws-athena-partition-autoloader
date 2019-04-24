# aws-athena-partition-autoloader
Automatically adds new partitions detected in S3 to an existing Athena table

# Purpose
Athena is fantastic for querying data in S3 and works especially well when the data is partitioned.  The issue comes when you have a lot of partitions and need to issue the `MSCK LOAD PARTITONS` command as it can take a long time.

This solution subscribes to S3 events on a bucket and detects when a new partition is created and then loads only that partition into Athena.  It uses a cache of the existing partitions to minimize the number of calls needed to Athena to query the parition list.

# Installing and Configuring

## AWS Setup

## Deploying to AWS
Before starting, you will need:
* The [AWS CLI](https://aws.amazon.com/cli/) installed and default credentials configured
* The [AWS SAM CLI](https://github.com/awslabs/aws-sam-cli) installed
* An existing S3 bucket where the AWS Lambda code will be deployed to by SAM
* An existing Athena table backed by content in S3 with at least 1 partition key
* This repo cloned

1. Run the *deploy.sh* script like

```
./deploy.sh <function_name> <s3 bucket region> <athena region> <action>  <s3 bucket to store lamba code in> <s3 bucket containing athena data> <S3 bucket for Athena results> <Athena database> <Athena table> <comma-seperated list of athena partition names> <AWS profile>
```

For Example:

```
./deploy.sh athena_loader_mytable eu-west-1 us-east-1 ALL lambda-sam-staging stage-audit-log aws-athena-query-results-123456789-us-east-1 audit_log_db api_audit_log 'destination_platform_id,date' staging
```

The list of partition keys must exactly match that which was defined on the table.

deploy.sh uses AWS SAM to package the AWS Lambda functions and then deploys them to AWS.  Everything is deployed as a Cloudformation Stack in the specified region.

| NOTE: If you don't have SAM installed, you can replace the SAM commands in the deploy script with `aws cloudformation package...` and `aws cloudformation deploy..` instead |
| --- |