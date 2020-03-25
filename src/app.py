from __future__ import print_function
import boto3
from botocore.exceptions import ClientError
import json
import urllib
import time
import os
import os.path
from pprint import pprint

# Global holding the AWS account ID this is executing in
account_id = 0

# The bucket could be in one region with the Athena DB being in a seperate region
athena_region = os.environ['ATHENA_REGION']

session = boto3.session.Session(region_name = athena_region)

#
# Get the current AWS account ID
#
def get_aws_account_id(session):
    global account_id

    if account_id == 0:
        account_id = session.client('sts').get_caller_identity()['Account']

    return account_id

#
# Submit a query to Athena; return the query ID
#
def submit_query(query, database, session):
    output_location = 's3://aws-athena-query-results-' + str(get_aws_account_id(session)) + "-" + session.region_name
    query_id = None
    response = None

    client = session.client('athena')

    try:
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': output_location,
                'EncryptionConfiguration': {
                    'EncryptionOption': 'SSE_S3'
                }
            }
        )
    except Exception as e:
        print("Error submitting query to Athena " + query + " (" + str(e) + ")")

    if response:
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print("Athena Query submitted successfully")
            query_id = response['QueryExecutionId']
        else:
            print("The response code was " + response['ResponseMetadata']['HTTPStatusCode'])

    return query_id

#
# Poll an existing query already submitted to Athena
#
def wait_for_query_to_complete(query_id, session):
    status = True
    client = session.client('athena')

    is_query_still_running = True
    while is_query_still_running:
        response = None
        try:
            response = client.get_query_execution(
                QueryExecutionId=query_id
            )
        except Exception as e:
            print("Error getting query execution for " + query_id + " (" + str(e) + ")")
            status = False

        if response and status:
            query_state = response['QueryExecution']['Status']['State']

            if query_state == 'FAILED':
                is_query_still_running = False
                
                if 'AlreadyExistsException' in response['QueryExecution']['Status']['StateChangeReason']:
                    print("Table partition already exists")
                    status = True
                else:
                    print("Athena query " + query_id + " failed")
                    status = False
            elif query_state == 'CANCELLED':
                print("Athena query " + query_id + " was cancelled")
                is_query_still_running = False
                status = False
            elif query_state == 'SUCCEEDED':
                print("Athena query " + query_id + " completed successfully")
                is_query_still_running = False
                status = True
            else:
                time.sleep(1)

    return status

#
# Get the results from a query that has executed
#
def get_query_results(query_id, session, header_row=True):
    status = True
    results = []
    skip_row_count=1

    if not header_row:
        skip_row_count=0
        
    client = session.client('athena')
    
    try:
        results_paginator = client.get_paginator('get_query_results')
        results_iter = results_paginator.paginate(QueryExecutionId=query_id)

        data_list = []

        for results_page in results_iter:
            for row in results_page['ResultSet']['Rows']:
                data_list.append(row['Data'])

        for datum in data_list[skip_row_count:]:
            results.append([x['VarCharValue'] for x in datum])
        
    except ClientError as e:
        print("Unexpected error getting query results: "+ e.response['Error']['Code'])
        
    return [tuple(x) for x in results]

#
# Query an Athena table to get the existing partitions
#
def get_existing_db_partitions(session, database, table_name):
    print("load_partition")
    query_results = None
    partitions = []

    get_partitions_sql = "SHOW PARTITIONS " + table_name

    query_id = submit_query(get_partitions_sql, database, session)

    if query_id:
        if wait_for_query_to_complete(query_id, session):
            query_results = get_query_results(query_id, session, False)
        else:
            print("ERROR running query to get existing partitions")

    # Query results come back as a tuple but we only care about the first val for partitions
    for part_info in query_results:
        partitions.append(part_info[0])

    return partitions

#
# Add a new partition to an Athena table
#
def add_partition(session, database, table_name, partition, bucket):
    current_key = 0
    status = False

    sql = 'ALTER TABLE ' + table_name + ' ADD PARTITION ('

    partition_key_vals = partition.split('/')
    # Filter out any prefix dirs from the key
    partition_key_vals = [p for p in partition_key_vals if "=" in p]
    partiton_key_count = len(partition_key_vals)

    for part in partition_key_vals:
        current_key += 1
        key,val = part.split('=')

        sql += key + " = '" + val + "'"

        if current_key != partiton_key_count:
            sql += ", "
        else:
            sql += ") "

    sql += "LOCATION 's3://" + bucket + "/" + partition + "';"

    print("Running sql: " + sql)

    query_id = submit_query(sql, database, session)

    if query_id:
        if wait_for_query_to_complete(query_id, session):
            status = True
        else:
            print("ERROR running query to add new partition")
            status = False
            
    return status

# 
# Write the list of table partitions to a cache file
#
def write_partition_cache(partitions, filename):
    with open(filename, 'w') as outfile:  
        json.dump(partitions, outfile)

#
# Load the list of partitions from the cache file
#
def load_partition_cache(filename):
    data = dict()

    with open(filename) as json_file:  
        data = json.load(json_file)

    return data

#
# Does an S3 key contain all the partition name keys
#
def partition_name_in_key(key, partition_keys):
    key_contains_all_partitions = False
    keys_found = 0
    partition_key_count = len(partition_keys)

    for part_name in partition_keys:
        if part_name in key:
            keys_found += 1

    if keys_found == partition_key_count:
        key_contains_all_partitions =  True

    return key_contains_all_partitions


# --------------- Main handler ------------------
def lambda_handler(event, context):
    '''
    Loads an athena partition if it is not already loaded
    '''
    
    partition_cache_file = '/tmp/partitions'

    database = os.environ['ATHENA_DATABASE']
    table_name = os.environ['ATHENA_TABLE']
    partition_keys = os.environ['PARTITION_KEYS'].split(',')
    
    # Log the the received event locally.
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event.
    bucket = event['Records'][0]['s3']['bucket']['name']
    s3_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key']).rstrip('/')
    size = event['Records'][0]['s3']['object']['size']
    
    print("The S3 key is " + s3_key)

    # Do we have a cached partition list?
    if not os.path.isfile(partition_cache_file):
        print("No partition cache file exists - creating it")
        existing_parts = get_existing_db_partitions(session, database, table_name)
        write_partition_cache(existing_parts, partition_cache_file)
    else:
        print("Partition cache file exists - loading it")
        existing_parts = load_partition_cache(partition_cache_file)

    # Now from the event, do we have this partition?

    # This will handle when we get data in the folder but ignore events for just the folder itself
    dirname = os.path.dirname(s3_key)

    # If we've removed the filename and we still have all the keys
    # then this is a valid partition
    if partition_name_in_key(dirname, partition_keys):
        print("Incoming event contains both parition keys")

        # is this in the cache?
        if dirname in existing_parts:
            print("A partition already exists for " + dirname)
        else:
            # We are ok if multiple lambdas try and add the same partition - it will fail
            # and we catch it when we get the results.
            add_partition(session, database, table_name, dirname, bucket)

            # Refresh the cache
            existing_parts = get_existing_db_partitions(session, database, table_name)
            write_partition_cache(existing_parts, partition_cache_file)

    return 'Success'
