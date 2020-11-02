from utility.util import is_none, ez_try_and_get, ez_get

import os
from time import sleep
import logging
from datetime import datetime, timezone, date
from decimal import *
import json
import concurrent.futures
import itertools
import threading

import boto3
from botocore.exceptions import ClientError

""" Note: imported below
import pandas as pd
import awswrangler as wr
"""

####################################################################################

 
# Opinion: Whoever designed the response schema hates developers
def standardize_athena_query_result(results, **kwargs):
    results = [x["Data"] for x in results['ResultSet']['Rows']]
    for n, row in enumerate(results):
        results[n] = [x['VarCharValue'] for x in row]

    if kwargs.get("output_lod"):
        headers = results.pop(0)
        output_lod = []
        for n, result_row in enumerate(results):
            output_lod.append({headers[i]:result_row[i] for i in range(0, len(result_row))})
        return output_lod

    return results


# Figure out pagination / 1000 row limit
# TODO implement a timeout in the query wait
def query_athena_table(sql_query, database, **kwargs):
    client = boto3.client('athena')
    queryStart = client.start_query_execution(
        QueryString=sql_query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={"OutputLocation": f"s3://{os.environ['AWS_ACCOUNT_ID']}-athena-query-results-bucket/"}
    )

    timeout_value = kwargs.get("timeout", 15)
    finished = False
    while not finished:
        query_status = client.get_query_execution(QueryExecutionId=queryStart["QueryExecutionId"])

        if query_status["QueryExecution"]["Status"]["State"] == "SUCCEEDED":
            results = client.get_query_results(QueryExecutionId=queryStart["QueryExecutionId"])
            finished = True

        elif timeout_value > ez_get(query_status, "QueryExecution", "Statistics", "TotalExecutionTimeInMillis"):
            logging.error(f"Query timed out with no response (timeout val: {timeout_value})")
            return None
        
        else:
            sleep(kwargs.get("wait_interval", 0.1))
            logging.info(query_status["QueryExecution"]["Status"]["State"])
            if query_status["QueryExecution"]["Status"]["State"] == "FAILED":
                logging.error(query_status["QueryExecution"]["Status"]["StateChangeReason"])
                return None

    results = standardize_athena_query_result(results, **kwargs)
    return results


################################### ~ Dynamo Operations ~  ############################################

# def decimal_default(obj):
#     # print(obj)
#     if isinstance(obj, Decimal):
#         return float(obj)
#     print( TypeError)

# Helper class to convert a DynamoDB item to JSON.
class DynamoReadEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        elif isinstance(o, date):
            return o.strftime("%m/%d/%Y"),
        return super(DecimalEncoder, self).default(o)

# Both reads and writes
def standardize_dynamo_query(input_data, **kwargs):
    if not isinstance(input_data, dict):
        logging.error("wrong data type for dynamodb")
        return None

    input_data['updatedAt'] = int(datetime.utcnow().timestamp())

    # TODO implement created logic
    if 'createdAt' not in input_data and kwargs.get("add_created"):
        input_data['createdAt'] = input_data['updatedAt']

    # Drop falsey keys, they break upserts
    input_data = {k:v for k,v in input_data.items() if not is_none(k)}

    # An AttributeValue may not contain an empty string
    for k, v in input_data.items():
        if is_none(v, keep_0=True) and not kwargs.get("skip_is_none"):
            input_data[k] = None
        elif isinstance(v, float):
            input_data[k] = Decimal(str(v))

    return input_data


# Converts timestamps back to human readable
def standardize_dynamo_output(output_data, **kwargs):
    datetime_keys = [key for key in output_data.keys() if key in ["updatedAt", "createdAt", 'ttl']]
    for key in datetime_keys:
        output_data[key] = datetime.fromtimestamp(output_data[key])#.replace(tzinfo=timezone.utc)

    return json.dumps(output_data, cls=DynamoReadEncoder) if kwargs.get("output") == "json" else output_data

# Note this will BY DEFAULT overwrite items with the same primary key (upsert)
def write_dynamodb_item(dict_to_write, table, **kwargs):
    table = boto3.resource('dynamodb').Table(table)
    dict_to_write = {"Item": standardize_dynamo_query(dict_to_write, **kwargs)}

    if kwargs.get("prevent_overwrites"): # TODO test
        dict_to_write["ConditionExpression"] =  "attribute_not_exists(#pk)",
        dict_to_write["ExpressionAttributeNames"] = {"#pk": kwargs["prevent_overwrites"]}

    try:
        table.put_item(**dict_to_write)
    except Exception as e:
        logging.error(e)
        logging.error(dict_to_write)
        return False

    if not kwargs.get("disable_print"): logging.info(f"Successfully did a Dynamo Write to {table}")
    return True


# Note this will overwrite items with the same primary key (upsert)
def batch_write_dynamodb_items(lod_to_write, table, **kwargs):
    table = boto3.resource('dynamodb').Table(table)

    with table.batch_writer() as batch:
        for item in lod_to_write:
            standard_item = standardize_dynamo_query(item, **kwargs)
            if standard_item:
                try:
                    batch.put_item(Item=standard_item)
                except Exception as e:
                    logging.error(f"{e} -- {standard_item}")

    logging.info(f"Succcessfully did a Dynamo Batch Write of length {len(lod_to_write)} to {table}")
    return True


"""
    A single Scan request can retrieve a maximum of 1 MB of data, then you have to paginate
    kwargs - Limit, ExclusiveStartKey
    ExclusiveStartKey - may not return what you think. make sure inherent sorting is what you think
"""
def scan_dynamodb(table, **kwargs):
    table = boto3.resource('dynamodb').Table(table)

    if kwargs.get("after") and isinstance(kwargs["after"], dict):
        kwargs["FilterExpression"] = "#ts BETWEEN :start and :end"
        kwargs["ExpressionAttributeNames"] = "#ts: " + list(kwargs["after"].keys())[0]
        kwargs["ExpressionAttributeValues"] = {":start":  {"N": list(kwargs.pop("after").values())[0]}, ":end": {"N": int(datetime.utcnow().timestamp())}}
    elif kwargs.get("after"):
        logging.error("Check your after kwarg")

    print(kwargs)
    result = table.scan(**kwargs)

    data_lod = result['Items']

    while 'LastEvaluatedKey' in result and result['Count'] < kwargs.get("limit", 10000000): # Pagination
        result = table.scan(ExclusiveStartKey=result['LastEvaluatedKey'])
        data_lod.extend(result['Items'])

    if not kwargs.get("disable_print"): logging.info(f"Succcessfully did a Dynamo List from {table}, found {result['Count']} results")
    for row in data_lod:
        row = standardize_dynamo_output(row)
    return data_lod


# returns a list of items
def parallel_scan_dynamodb(TableName, **kwargs):
    """
    Generates all the items in a DynamoDB table.

    :param dynamo_client: A boto3 client for DynamoDB.
    :param TableName: The name of the table to scan.

    Other keyword arguments will be passed directly to the Scan operation.
    See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.scan

    This does a Parallel Scan operation over the table.

    Source: https://alexwlchan.net/2020/05/getting-every-item-from-a-dynamodb-table-with-python/
    """
    # How many segments to divide the table into?  As long as this is >= to the
    # number of threads used by the ThreadPoolExecutor, the exact number doesn't
    # seem to matter.
    dynamo_client = boto3.resource("dynamodb").meta.client

    total_segments = 25

    # How many scans to run in parallel?  If you set this really high you could
    # overwhelm the table read capacity, but otherwise I don't change this much.
    max_scans_in_parallel = 5

    # Schedule an initial scan for each segment of the table.  We read each
    # segment in a separate thread, then look to see if there are more rows to
    # read -- and if so, we schedule another scan.
    tasks_to_do = [
        {
            **kwargs,
            "TableName": TableName,
            "Segment": segment,
            "TotalSegments": total_segments,
        }
        for segment in range(total_segments)
    ]

    # Make the list an iterator, so the same tasks don't get run repeatedly.
    scans_to_run = iter(tasks_to_do)

    with concurrent.futures.ThreadPoolExecutor() as executor:

        # Schedule the initial batch of futures.  Here we assume that
        # max_scans_in_parallel < total_segments, so there's no risk that
        # the queue will throw an Empty exception.
        futures = {
            executor.submit(dynamo_client.scan, **scan_params): scan_params
            for scan_params in itertools.islice(scans_to_run, max_scans_in_parallel)
        }

        while futures:
            # Wait for the first future to complete.
            done, _ = concurrent.futures.wait(
                futures, return_when=concurrent.futures.FIRST_COMPLETED
            )

            for fut in done:
                yield from fut.result()["Items"]

                scan_params = futures.pop(fut)

                # A Scan reads up to N items, and tells you where it got to in
                # the LastEvaluatedKey.  You pass this key to the next Scan operation,
                # and it continues where it left off.
                try:
                    scan_params["ExclusiveStartKey"] = fut.result()["LastEvaluatedKey"]
                except KeyError:
                    break
                tasks_to_do.append(scan_params)

            # Schedule the next batch of futures.  At some point we might run out
            # of entries in the queue if we've finished scanning the table, so
            # we need to spot that and not throw.
            for scan_params in itertools.islice(scans_to_run, len(done)):
                futures[executor.submit(dynamo_client.scan, **scan_params)] = scan_params



# If you set a composite primary key (both a HASH and RANGE, both a partition key and sort key), YOU NEED BOTH to getItem and updateItem
def get_dynamodb_item(primary_key_dict, table_name, **kwargs):
    if not isinstance(primary_key_dict, dict):
        raise ValueError("You need to pass a dict of primary_key:value and also a sort key if you have a composite")

    table = boto3.resource('dynamodb').Table(table_name)

    result = table.get_item(Key=primary_key_dict)
    if not kwargs.get("disable_print"): logging.info(f"Successfully did a Dynamo Get from {table_name}: {result.get('Item', None)}")
    return standardize_dynamo_output(result.get('Item')) if result.get("Item") else None


def delete_dynamodb_item(unique_key, key_value, table_name, **kwargs):
    table = boto3.resource('dynamodb').Table(table_name)

    result = table.delete_item(Key={unique_key:key_value})
    if not kwargs.get("disable_print"): logging.info(f"Succcessfully did a Dynamo Delete of key {key_value} from {table_name}, status_code {ez_get(result, 'ResponseMetadata', 'HTTPStatusCode')}")

# TODO test
def increment_dynamodb_item_counter(primary_key_value, counter_attr, table_name, **kwargs):
    table = boto3.resource('dynamodb').Table(table_name)

    update_item_dict = {
        "Key": primary_key_value,
        "UpdateExpression": f"SET #{counter_attr} = #{counter_attr} + :amount",
        "ExpressionAttributeValues": {f"#{counter_attr}": "{counter_attr}"},
        "ExpressionAttributeNames": {":amount": str(kwargs.get("increment_by", 1))},
        "ReturnValues": "UPDATED_OLD",
    }
    result = table.update_item(**update_item_dict)
    if not kwargs.get("disable_print"): logging.info(f"Succcessfully did a Dynamo Increment from {table_name}")
    return result.get('Attributes')


def upsert_dynamodb_item(key_dict, dict_of_attributes, table_name, **kwargs):
    table = boto3.resource('dynamodb').Table(table_name)
    dict_of_attributes = standardize_dynamo_query(dict_of_attributes, **kwargs)

    string_of_attributes = "SET "
    for k in dict_of_attributes.keys():
        string_of_attributes += f"#{k} = :{k}, "
    string_of_attributes = string_of_attributes.rstrip(", ")

    update_item_dict = {
        "Key": key_dict,
        "UpdateExpression": string_of_attributes,
        "ExpressionAttributeValues": {f":{k}": v for k,v in dict_of_attributes.items()},
        "ExpressionAttributeNames": {f"#{k}": k for k in dict_of_attributes},
        "ReturnValues": "UPDATED_OLD",
    }

    result = table.update_item(**update_item_dict)

    logging.info(f"Succcessfully did a Dynamo Upsert to {table_name}")
    if kwargs.get("print_old_values"):
        logging.info(f"The updates that were attributed (and their OLD VALUES): {result.get('Attributes', None)}")

    return result.get('Attributes')


# TODO implement
# def query_dynamodb_table(operation_parameters_dict, table, **kwargs):
#     table = boto3.resource('dynamodb').Table(table)
    # dict_of_attributes = standardize_dynamo_query(dict_of_attributes, **kwargs)

#     operation_parameters_dict["TableName"] = table
#     result = table.query(**operation_parameters_dict)
#     # client = boto3.client('dynamodb')
    # paginator = client.get_paginator('query')
    # operation_parameters = {
      # 'TableName': table,
    #   'FilterExpression': 'bar > :x AND bar < :y',
    #   'ExpressionAttributeValues': {
    #     ':x': {'S': '2017-01-31T01:35'},
    #     ':y': {'S': '2017-01-31T02:08'},
    #   }
    # }

    # page_iterator = paginator.paginate(**operation_parameters_dict)
    # for page in page_iterator:
    #     # do something
    #     print(page)
    # # result = table.query(
    #     KeyConditionExpression=boto3.dynamodb.conditions.Key(primary_key).eq(primary_key_value)
    # )
    # if not kwargs.get("disable_print"): logging.info(f"Succcessfully did a Dynamo Query on {table}")
    # return data
# TODO decimal encoding? https://github.com/serverless/examples/blob/master/aws-python-rest-api-with-dynamodb/todos/decimalencoder.py
# TODO Upsert https://github.com/serverless/examples/blob/master/aws-python-rest-api-with-dynamodb/todos/update.py



#################### ~ S3 Specific ~ ##########################################


def get_s3_bucket_file_count(bucket_name, path):
    bucket = boto3.resource("s3").Bucket(bucket_name)
    return sum(1 for _ in bucket.objects.all())

# The path should be `folder/` NOT `/folder`
# MaxKeys = number of results per page, NOT number of total results
def list_s3_bucket_contents(bucket_name, path, **kwargs):
    client = boto3.client("s3")
    storage_classes = ["STANDARD"] if kwargs.get("ignore_glacier") else ["STANDARD", "STANDARD_IA", "GLACIER"]
    filter_args = {"Bucket":bucket_name, "Prefix": path}
    if "start_after" in kwargs: filter_args["StartAfter"] = kwargs["start_after"]
    if "limit" in kwargs: filter_args["MaxKeys"] = kwargs["limit"]
    print(filter_args)

    response = client.list_objects_v2(**filter_args)
    return [x.get("Key") for x in response["Contents"]]


# def list_s3_bucket_contents(bucket_name, path, **kwargs):
#     bucket = boto3.resource("s3").Bucket(bucket_name)
#     storage_classes = ["STANDARD"] if kwargs.get("ignore_glacier") else ["STANDARD", "STANDARD_IA", "GLACIER"]
#     filter_args = {"Prefix": path}
#     if "start_on" in kwargs: filter_args["StartAfter"] = kwargs["start_on"]
#     if "limit" in kwargs: filter_args["MaxKeys"] = kwargs["limit"]
#     print(filter_args)
#
#     # return [x.key for x in bucket.objects.filter(**filter_args).limit(kwargs.get("limit", None)) if x.storage_class in storage_classes]
#         # return [x.key for x in bucket.objects.filter(**filter_args).limit(kwargs["limit"])]
#     client = boto3.client("s3")
#     # return [x.key for x in bucket.objects.filter(**filter_args)]
#     response = client.list_objects_v2(
#         bucket=bucket_name
#         prefix=path,
#         **filter_args
#     )



# default encoding of ISO-8859-1? TODO
def get_s3_file(bucket_name, filename, **kwargs):
    try:
        s3_obj = boto3.client("s3").get_object(Bucket=bucket_name, Key=filename)["Body"]
        return s3_obj if kwargs.get("raw") else s3_obj.read().decode('utf-8')
    except Exception as e:
        logging.error(e)
        raise e


# for use with `for line in body`
def stream_s3_file(bucket_name, filename, **kwargs):
    s3 = boto3.resource('s3')
    s3_object = s3.Object(Bucket=bucket_name, Key=filename)
    return s3_object.get()['Body'] #body returns streaming string


def write_s3_file(bucket_name, filename, json_data, **kwargs):
    try:
        s3_object = boto3.resource("s3").Object(bucket_name, filename)
        response = s3_object.put(Body=(bytes(json.dumps(json_data).encode("UTF-8"))))
        status_code = ez_try_and_get(response, 'ResponseMetadata', 'HTTPStatusCode')
        if kwargs.get("enable_print"): logging.info(f"Successful write to {filename} / {status_code}")
        return status_code
    except Exception as e:
        # logging.error(e)
        logging.error(e, bucket_name, filename, json_data)


#http://ls.pwd.io/2013/06/parallel-s3-uploads-using-boto-and-threads-in-python/
# list of tuples
def parallel_write_s3_files(bucket_name, file_lot):
    boto3.client('s3')
    for file_tuple in file_lot:
        t = threading.Thread(target = write_s3_file, args=(bucket_name, file_tuple[0], file_tuple[1])).start()

    logging.info(f"Parallel write to S3 Bucket {bucket_name} has finished")


def delete_s3_file(bucket_name, filename, **kwargs):
    try:
        response = boto3.resource("s3").Object(bucket_name, filename).delete()
        status_code = ez_try_and_get(response, 'ResponseMetadata', 'HTTPStatusCode')
        if not kwargs.get("disable_print"): logging.info(f"Successful delete of {filename} - Status Code: {status_code}")
        return status_code

    except ClientError as e:
        logging.error(e)
        return e

"""
Minimums for storage classes:
    Normal - None
    1Z Infrequent - 30 days
    Glacier - 90 days
"""
def move_s3_file_to_glacier(bucket_name, path):
    s3 = boto3.client('s3')

    s3.copy({"Bucket": bucket_name, "Key": path}, bucket_name, path,
        ExtraArgs={'StorageClass': 'GLACIER', 'MetadataDirective': 'COPY'})
    return



# TODO implement this to list and download every content of a s3 bucket
# you can't just download you have to list and then iterate over
# https://stackoverflow.com/questions/31918960/boto3-to-download-all-files-from-a-s3-bucket
# def download_dir(client, resource, dist, local='/tmp', bucket='your_bucket'):
#     paginator = client.get_paginator('list_objects')
#     for result in paginator.paginate(Bucket=bucket, Delimiter='/', Prefix=dist):
#         if result.get('CommonPrefixes') is not None:
#             for subdir in result.get('CommonPrefixes'):
#                 download_dir(client, resource, subdir.get('Prefix'), local, bucket)
#         for file in result.get('Contents', []):
#             dest_pathname = os.path.join(local, file.get('Key'))
#             if not os.path.exists(os.path.dirname(dest_pathname)):
#                 os.makedirs(os.path.dirname(dest_pathname))
#             resource.meta.client.download_file(bucket, file.get('Key'), dest_pathname)


# TODO difference between
"""
    s3 = boto3.resource("s3")
    s3.Object(bucket, filename)
    return obj.get()["Body"].read().decode("utf-8")
and
    s3 = boto3.client("s3")
    csv_file = s3.get_object(Bucket=bucket, Key=filename)["Body"]
"""



###################### ~ SQS Specific ~ ###################################################


def sqs_send_message(data, queue_name):
    SQS = boto3.client("sqs")
    q = SQS.get_queue_url(QueueName=queue_name).get('QueueUrl')

    logging.debug(f"Sending data: {data}")
    params = {"QueueUrl": q, "MessageBody": data}
    if "fifo" in queue_name:
        params["MessageGroupId"] = "foobar"
    resp = SQS.send_message(**params)
    status_code = ez_try_and_get(resp, "ResponseMetadata", "HTTPStatusCode")
    logging.info(f"Write result status: {status_code}")

    return resp

# Does not support FIFO
def sqs_send_batched_message(data_lod, id_key, queue_name):
    SQS = boto3.client("sqs")
    q = SQS.get_queue_url(QueueName=queue_name).get('QueueUrl')

    if "Id" not in data_lod[0]:
        data_lod = [{'Id':item[id_key], "MessageBody": json.dumps(item)} for item in data_lod]


    response = SQS.send_message_batch(
        QueueUrl=q,
        Entries=data_lod
    )
    # Print out any failures
    print(response.get('Failed'))


def sqs_read_message(queue_name, **kwargs):
    message_number = kwargs.get("Message_Number", 1)

    SQS = boto3.client("sqs")
    q = SQS.get_queue_url(QueueName=queue_name).get('QueueUrl')

    data = SQS.receive_message(QueueUrl=q, MaxNumberOfMessages=message_number)

    if not data.get("Messages"):
        logging.warning(f"As a warning there are no messages in the queue")
        return None

    response_number = len(ez_try_and_get(data, "Messages"))
    status_code = ez_try_and_get(data, "ResponseMetadata", "HTTPStatusCode")
    logging.info(f"Read result status: {status_code}")

    messages = [ez_try_and_get(data, "Messages", x, "Body") for x in range(response_number)]
    messages = [json.loads(x) if isinstance(x, str) else x for x in messages]

    if message_number != response_number:
        logging.warning(f"You requested {message_number} and you got {response_number} messages")

    if messages and message_number == 1 and not kwargs.get("requeue_message"):

        receiptHandle = ez_try_and_get(data, "Messages", 0, "ReceiptHandle")
        resp = SQS.delete_message(QueueUrl=q, ReceiptHandle=receiptHandle)
        status_code2 = ez_try_and_get(data, "ResponseMetadata", "HTTPStatusCode")
        logging.info(f"Deleted queue message (after reading). Status code was {status_code2}")

    return messages[0] if response_number == 1 else messages



########################### ~ RDS Aurora Serverless Data API Specific ~ ###################################################


def aurora_execute_sql(db, sql, **kwargs):
    client = boto3.client('rds-data')
    result = client.execute_statement(
        secretArn=os.environ["SM_SECRET_ARN"],
        database=db,
        resourceArn=os.environ["DB_ARN"],
        sql=sql,
        parameters=[]
    )
    if not kwargs.get("disable_print"): logging.info(f"Successful execution: {sql} / {len(result)}")

    return result


########################### ~ S3 Data Lake Specific ~ ###################################################


def write_data_to_parquet_in_s3(data, s3_path, **kwargs):
    import pandas as pd
    import awswrangler as wr


    if isinstance(data, list) and isinstance(data[0], dict):
        data = pd.DataFrame(data) # convert_to_dataframe(df, )
    
    s3_path = "s3://" + s3_path if not s3_path.startswith("s3://") else s3_path

    wr.s3.to_parquet(
        df=data,
        path=s3_path,
        dataset=True,           # Stores as parquet dataset instead of 'ordinary file'
        mode=kwargs.get("write_mode", "overwrite"), # Could be append, overwrite or overwrite_partitions
        database=kwargs.get("database", None),      # Optional, only with you want it available on Athena/Glue Catalog
        table=kwargs.get("table", None),
        compression=kwargs.get("compression", "snappy"),   # TODO check if this is ideal
        # dtype                 # TODO Dictionary of columns names and Athena/Glue types to be casted. Useful when you have columns with undetermined or mixed data types. (e.g. {‘col name’: ‘bigint’, ‘col2 name’: ‘int’})
        max_rows_by_file=kwargs.get("max_rows_by_file", None), # If set = n, every n rows, split into a new file. If None, don't split
        partition_cols=kwargs.get("partition_cols_list", None)
    )

    logging.info(f"Write was successful to path {s3_path}")


########################### ~ CloudWatch Specific ~ ###################################################

# query = "fields @timestamp, @message | parse @message \"username: * ClinicID: * nodename: *\" as username, ClinicID, nodename | filter ClinicID = 7667 and username='simran+test@abc.com'"
# log_group = '/aws/lambda/NAME_OF_YOUR_LAMBDA_FUNCTION'
def cw_query_logs(query, log_group, lookback_hours):
    client = boto3.client('logs')
    start_query_response = client.start_query(
        logGroupName=log_group,
        startTime=int((datetime.today() - timedelta(hours=lookback_hours)).timestamp()),
        endTime=int(datetime.now().timestamp()),
        queryString=query,
    )

    response = None
    while response == None or response['status'] == 'Running':
        time.sleep(1)
        response = client.get_query_results(
            queryId=start_query_response['queryId']
        )

    return response["results"]
    # for invoke_logs in response['results']:
        # for log_row in invoke_logs