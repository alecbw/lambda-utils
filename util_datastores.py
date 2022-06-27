from utility.util import is_none, ez_try_and_get, ez_get, ez_split, startswith_replace

import sys
import os
from time import sleep
import logging
from datetime import datetime, timezone, date, timedelta
import time
from decimal import *
import json
import concurrent.futures
import itertools
import threading
import random
import string
import csv
import timeit
import ast
from pprint import pprint
from io import StringIO
from typing import Callable, Iterator, Union, Optional, List
from collections import defaultdict

import boto3
from botocore.exceptions import ClientError
from botocore.client import Config

""" Note: imported below
import pandas as pd
import awswrangler as wr
"""


######################## ~ Athena Queries ~ #############################################


def coerce_float(maybe_float):
    try:
        return float(maybe_float)
    except Exception as e:
        pass


def convert_athena_array_cols(data_lod, **kwargs):
    # if not kwargs.get("convert_array_cols"):
    #     return data_lod

    for n, row in enumerate(data_lod):
        data_lod[n] = convert_athena_row_types(row, **kwargs)

    return data_lod


def convert_athena_row_types(row, **kwargs):
    for k,v in row.items():
        if k not in kwargs.get("convert_array_cols", []) and v and v.isdigit():
            row[k] = int(v)
        elif k not in kwargs.get("convert_array_cols", []) and coerce_float(v):
            row[k] = float(v)
        elif k not in kwargs.get("convert_array_cols", []):
            continue
        elif v == '[]' or not v:
            row[k] = []
        else:
            row[k] = v.strip('][').split(', ')

    return row


# Opinion: Whoever designed the response schema hates developers
def standardize_athena_query_result(results, **kwargs):
    result_lol = [x["Data"] for x in results['ResultSet']['Rows']]
    for n, row in enumerate(result_lol):
        result_lol[n] = [x.get('VarCharValue', None) for x in row] # NOTE: the .get(fallback=None) WILL cause problems if you have nulls in non-string cols

    if not kwargs.get("output_lod"):
        return result_lol
    elif kwargs.get("output_lod"):
        headers = kwargs.get("headers") or result_lol.pop(0)

        result_lod = []
        for n, result_row in enumerate(result_lol):
            result_row_dict = {headers[i]:result_row[i] for i in range(0, len(result_row))}
            if not kwargs.get("skip_type_conversion"):
                result_row_dict = convert_athena_row_types(result_row_dict, **kwargs)
            result_lod.append(result_row_dict)

        return result_lod



# about 4s per 10k rows, with a floor of ~0.33s if only one page
def paginate_athena_response(client, execution_id: str, **kwargs):# -> AthenaPagedResult:
    """
    Returns the query result for the provided page as well as a token to the next page if there are more
    results to retrieve for the query.

    EMPTY_ATHENA_RESPONSE = {'UpdateCount': 0, 'ResultSet': {'Rows': [{'Data': [{}]}]}}
    """
    paginator = client.get_paginator('get_query_results')

    response_iterator = paginator.paginate(
        QueryExecutionId=execution_id,
        PaginationConfig={
            'MaxItems': kwargs.get("max_results", 100000),
            'PageSize': 1000,
            'StartingToken': kwargs.get("pagination_starting_token", None),
    })

    results = []
    # Iterate through pages. The NextToken logic is handled for you.
    for n, page in enumerate(response_iterator):
        logging.debug(f"Now on page {n}, rows on this page: {len(page['ResultSet']['Rows'])}")

        results += standardize_athena_query_result(page, **kwargs)

        if not results:
            break

        if kwargs.get("output_lod"):
            kwargs["headers"] = list(results[0].keys()) # prevent parser from .pop(0) after 1st page

    return results


def query_athena_table(sql_query, database, **kwargs):
    if database not in sql_query:
        logging.warning("The provided database is not in your provided SQL query")

    if kwargs.get("time_it"): start_time = timeit.default_timer()

    client = boto3.client('athena')
    query_started = client.start_query_execution(
        QueryString=sql_query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={"OutputLocation": kwargs.get("result_bucket", f"s3://{os.environ['AWS_ACCOUNT_ID']}-athena-query-results-bucket/")}
    )

    if kwargs.get("dont_wait_for_query_result"):
        return True

    timeout_value = kwargs.get("timeout", 15) * 1000 # bc its in milliseconds
    finished = False

    while not finished:
        query_in_flight = client.get_query_execution(QueryExecutionId=query_started["QueryExecutionId"])
        query_status = query_in_flight["QueryExecution"]["Status"]["State"]

        if query_status == 'SUCCEEDED':
            s3_result_path = query_in_flight['QueryExecution']['ResultConfiguration']['OutputLocation'].replace("s3://", "")
            s3_result_dict = {"bucket": s3_result_path[:s3_result_path.rfind("/")], "filename": s3_result_path[s3_result_path.rfind("/")+1:], "data_scanned_mb": ez_get(query_in_flight, 'QueryExecution', 'Statistics', 'DataScannedInBytes') / 1_000_000}
            finished = True
        elif query_status in ['FAILED', 'CANCELLED']: # TODO test cancelled
            logging.error(query_in_flight['QueryExecution']['Status']['StateChangeReason'])
            return None
        elif timeout_value < ez_get(query_in_flight, "QueryExecution", "Statistics", "TotalExecutionTimeInMillis"):
            logging.error(f"Query timed out with no response (timeout val: {timeout_value})")
            return None
        else:
            sleep(kwargs.get("wait_interval", 0.01))


    if kwargs.get("time_it"): logging.info(f"Query execution time (NOT including pagination/file-handling) - {round(timeit.default_timer() - start_time, 4)} seconds")

    if kwargs.get("return_s3_path"):
        s3_result_dict["entry_count"] = get_row_count_of_s3_csv(s3_result_dict['bucket'], s3_result_dict['filename'])
        result = s3_result_dict
    elif kwargs.get("return_s3_file"): # as lod
        s3_result_dict["data"] = convert_athena_array_cols(get_s3_file(s3_result_dict["bucket"], s3_result_dict["filename"], convert_csv=True), **kwargs)
        result = s3_result_dict
    else:
        result = paginate_athena_response(client, query_started["QueryExecutionId"], **kwargs)

    if kwargs.get("time_it"): logging.info(f"Query execution time (all-in) - {round(timeit.default_timer() - start_time, 4)} seconds")

    logging.info(f"Athena query has finished. Data scanned: {ez_get(query_in_flight, 'QueryExecution', 'Statistics', 'DataScannedInBytes') / 1_000_000} MB. Data return will be {next((x for x in ['return_s3_path', 'return_s3_file', 'output_lod'] if x in kwargs.keys()), 'lol - default')}")

    return result


def get_athena_named_queries() -> List[dict]:
    client = boto3.client('athena')

    query_id_resp = client.list_named_queries(
        MaxResults=50, # max 50 per page
    )
    saved_queries = client.batch_get_named_query(NamedQueryIds=query_id_resp['NamedQueryIds'])['NamedQueries']

    while query_id_resp.get("NextToken"):
        query_id_resp = client.list_named_queries(
            NextToken=query_id_resp["NextToken"],
            MaxResults=50,
        )
        saved_queries += client.batch_get_named_query(NamedQueryIds=query_id_resp['NamedQueryIds'])['NamedQueries']

    print(f"A total of {len(saved_queries)} saved queries were found")
    return saved_queries

    # return saved_queries['NamedQueries']

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
# TODO refactor for readability
def standardize_dynamo_query(input_data, **kwargs):

    if not isinstance(input_data, dict):
        logging.error(f"Wrong data type for dynamodb - you input {type(input_data)}")
        return None

    if input_data.get("created_at") or input_data.get("updated_at"):
        if input_data.get("created_at") and str(input_data['created_at']).isdigit():
            input_data['created_at'] = int(input_data['created_at'])
        if not kwargs.get("skip_updated"):
            input_data['updated_at'] = int(input_data.get("updated_at", input_data.get('created_at')))
    else:
        if not kwargs.get("skip_updated"):
            input_data['updatedAt'] = int(datetime.utcnow().timestamp())
        elif "updatedAt" in input_data:
            input_data['updatedAt'] = int(input_data['updatedAt'])

        if kwargs.get("add_created") and 'createdAt' not in input_data:
            input_data['createdAt'] = input_data['updatedAt']

    for k, v in input_data.items():
        if is_none(k):  # Drop falsey keys (and their vals), they break upserts
            logging.warning(f"Dropping falsey key {k}")
            del input_data[k]
        elif is_none(v, keep_0=True) and not kwargs.get("skip_is_none"):  # (An AttributeValue may not contain an empty string)
            input_data[k] = None
        elif isinstance(v, float):
            input_data[k] = Decimal(str(v))

    return input_data


# Converts timestamps back to human readable
def standardize_dynamo_output(output_data, **kwargs):
    if not output_data:
        return output_data

    datetime_keys = [key for key in output_data.keys() if key in ["updatedAt", "createdAt", "updated_at", "created_at", 'ttl']]
    for key in datetime_keys:
        if not output_data[key]:
            return "" if kwargs.get("output") == "datetime_str" else None
        elif output_data[key].isdigit():
            output_data[key] = datetime.fromtimestamp(output_data[key])#.replace(tzinfo=timezone.utc)

        if kwargs.get("output") == "datetime_str":
            output_data[key] = output_data[key].strftime('%Y-%m-%d %H:%M:%S')

    if kwargs.get("output") == "json": # each dict will be JSON, but not the overall list
        return json.dumps(output_data, cls=DynamoReadEncoder)
    else:
        return output_data


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


"""
Note this will overwrite items with the same primary key (upsert)
If you pass a lod of len > 100, it will quietly split it to mini-batches of 100 each
"""
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

    logging.info(f"Successfully did a Dynamo Batch Write of length {len(lod_to_write)} to {table}")
    return True


"""
    A single Scan request can retrieve a maximum of 1 MB of data, then you have to paginate
    kwargs - Limit, after, ExclusiveStartKey
    ExclusiveStartKey - may not return what you think. make sure inherent sorting is what you think
    after - a one-item dict, where k is the timestamp col name and v is the timestamp to start the comparison at
            after={"timestamp": 1603678545}
"""
def scan_dynamodb(table, **kwargs):
    table = boto3.resource('dynamodb').Table(table)

    if kwargs.get("after") and isinstance(kwargs["after"], dict) and len(kwargs["after"]) == 1:
        kwargs["FilterExpression"] = "#ts > :start"
        kwargs["ExpressionAttributeNames"] = {"#ts": list(kwargs["after"].keys())[0]}
        kwargs["ExpressionAttributeValues"] = {":start": list(kwargs.pop("after").values())[0]} #, ":end": {"N": int(datetime.utcnow().timestamp())}}
    elif kwargs.get("after"):
        logging.error("Check your after kwarg")

    scan_kwarg_key_list = ["TableName", "IndexName", "AttributesToGet", "Limit", "Select", "ScanFilter", "ConditionalOperator", "ExclusiveStartKey", "ReturnConsumedCapacity", "TotalSegments", "Segment", "ProjectionExpression", "FilterExpression", "ExpressionAttributeNames", "ExpressionAttributeValues", "ConsistentRead"]
    scan_kwargs = {k:v for k,v in kwargs.items() if k in scan_kwarg_key_list}
    if scan_kwargs:
        logging.info(f"The following kwargs will be applied to the scan {scan_kwargs}")

    result = table.scan(**scan_kwargs)

    data_lod = result['Items']

    while 'LastEvaluatedKey' in result and result['Count'] < kwargs.get("Limit", 10000000): # Pagination
        kwargs["ExclusiveStartKey"] = result['LastEvaluatedKey']
        result = table.scan(**kwargs)
        data_lod.extend(result['Items'])
        print('extending')

    if not kwargs.get("disable_print"): logging.info(f"Successfully did a Dynamo Scan from {table}, found {result['Count']} results")

    for n, row in enumerate(data_lod):
        data_lod[n] = standardize_dynamo_output(row, **kwargs)

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


def get_dynamodb_item_from_index(primary_key_dict, table, index_name, **kwargs):
    dict_of_attributes = standardize_dynamo_query(primary_key_dict, skip_updated=True, **kwargs)

    string_of_attributes = ""
    for k in dict_of_attributes.keys():
        string_of_attributes += f"#{k} = :{k} AND "
    string_of_attributes = string_of_attributes.rstrip(" AND ")

    query_dict = {
        "KeyConditionExpression": string_of_attributes,
        "ExpressionAttributeNames": {f"#{k}": k for k in dict_of_attributes},
        "ExpressionAttributeValues": {f":{k}": v for k,v in dict_of_attributes.items()},
        "IndexName": index_name,
    }

    results = table.query(**query_dict)

    if not results.get("Items"):
        return {}
    elif len(results.get('Items')) == 1:
        return results['Items'][0]
    else:
        return [standardize_dynamo_output(x) for x in results['Items']]


# If you set a composite primary key (both a HASH and RANGE, both a partition key and sort key), YOU NEED BOTH to getItem and updateItem
def get_dynamodb_item(primary_key_dict, table_name, **kwargs):
    if not isinstance(primary_key_dict, dict):
        raise ValueError("You need to pass a dict of primary_key:value and also a sort key if you have a composite")

    table = boto3.resource('dynamodb').Table(table_name)

    if kwargs.get("index"):
        result = get_dynamodb_item_from_index(primary_key_dict, table, kwargs.pop("index"), **kwargs)
    else:
        result = table.get_item(Key=primary_key_dict)
        result = standardize_dynamo_output(result.get('Item')) # if result.get("Item") else None

    if not kwargs.get("disable_print"): logging.info(f"Successfully did a Dynamo Get from {table_name}: {result}")
    return result



def delete_dynamodb_item(unique_key, key_value, table_name, **kwargs):
    table = boto3.resource('dynamodb').Table(table_name)

    result = table.delete_item(Key={unique_key:key_value})
    if not kwargs.get("disable_print"): # note: it will return status code 200 even if the key wasn't in the table to begin with
        logging.info(f"Successfully did a Dynamo Delete of key {key_value} from {table_name}, status_code {ez_get(result, 'ResponseMetadata', 'HTTPStatusCode')}")


# TODO test. Alternate implementation: https://github.com/fernando-mc/nandolytics/blob/master/record.py
def increment_dynamodb_item_counter(primary_key_value, counter_attr, table_name, **kwargs):
    table = boto3.resource('dynamodb').Table(table_name)

    update_item_dict = {
        "Key": primary_key_value,
        "UpdateExpression": "SET #counter = #counter + :amount",
        "ExpressionAttributeNames": {"#counter": counter_attr},
        "ExpressionAttributeValues": {":amount": int(kwargs.get("increment_by", 1))},
        "ReturnValues": "UPDATED_OLD",
    }
    result = table.update_item(**update_item_dict)
    if not kwargs.get("disable_print"): logging.info(f"Successfully did a Dynamo Increment from {table_name}")
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

    logging.info(f"Successfully did a Dynamo Upsert to {table_name}")
    if kwargs.get("print_old_values"):
        logging.info(f"The updates that were attributed (and their OLD VALUES): {result.get('Attributes', None)}")

    return result.get('Attributes')


#################### ~ S3 Specific ~ ##########################################


def get_s3_bucket_file_count(bucket_name, path):
    bucket = boto3.resource("s3").Bucket(bucket_name)
    if not path:
        return sum(1 for _ in bucket.objects.all())
    else:
        return sum(1 for _ in bucket.objects.filter(Prefix=path.lstrip("/")))

"""
    The path should be `folder/` NOT `/folder`
    MaxKeys = number of results per page, NOT number of total results
    It would appear the list is ordered (ie if you use limit=1 it will always be the same)
"""
def list_s3_bucket_contents(bucket_name, path, **kwargs):
    storage_classes = ["STANDARD"] if kwargs.get("ignore_glacier") else ["STANDARD", "STANDARD_IA", "GLACIER"]

    client = boto3.client("s3")
    filter_args = {"Bucket":bucket_name, "Prefix": path.lstrip("/")}

    if "start_after" in kwargs:
        filter_args["StartAfter"] = kwargs["start_after"]
    if "limit" in kwargs:
        filter_args["MaxKeys"] = kwargs["limit"]

    response = client.list_objects_v2(**filter_args)
    if response.get("Contents"):
        return [x.get("Key") for x in response["Contents"]]
    return []


# Via S3 Select. Note: intra-AWS data transfer (e.g. Lambda <> S3) is much faster than egress, so this optimization is less impactful to intra-AWS use cases
# DOES NOT INCLUDE NULL ROWS
def get_row_count_of_s3_csv(bucket_name, path):
    sql_stmt = """SELECT count(*) FROM s3object """
    try:
        req = boto3.client('s3').select_object_content(
            Bucket=bucket_name,
            Key=path,
            ExpressionType="SQL",
            Expression=sql_stmt,
            InputSerialization = {"CSV": {"FileHeaderInfo": "Use", "AllowQuotedRecordDelimiter": True}},
            OutputSerialization = {"CSV": {}},
        )
        row_count = next((int(x["Records"]["Payload"]) for x in req["Payload"] if x.get("Records")), 0)
    except ClientError as e:  # specifically for OverMaxRecordSize Error, where characters in one cell exceed maxCharsPerRecord (1,048,576)
        logging.error(e)
        return 0

    return row_count


# default encoding of ISO-8859-1? TODO
def get_s3_file(bucket_name, filename, **kwargs):
    if "/" in bucket_name:
        logging.warning("~~You probably want to remove the filepath from the bucket_name. It's _just_ the bucket ~~")
    try:
        s3_obj = boto3.client("s3").get_object(Bucket=bucket_name, Key=filename.lstrip("/"))["Body"]
        if kwargs.get("raw"):
            return s3_obj
        elif kwargs.get("convert_csv"):
            csv.field_size_limit(sys.maxsize) # circumvents `field larger than field limit (131072)` Error
            return list(csv.DictReader(s3_obj.read().decode('utf-8').splitlines(True), skipinitialspace=True))
            # return [{k:v for k, v in row.items()} for row in csv.DictReader(s3_obj.read().decode('utf-8').splitlines(True), skipinitialspace=True)]
        elif kwargs.get("convert_json"):
            return json.loads(s3_obj.read().decode('utf-8'))
        else:
            return s3_obj.read().decode('utf-8')

    except Exception as e:
        logging.error(e)
        raise e # feels redundant TODO


# for use with `for line in body`
def stream_s3_file(bucket_name, filename, **kwargs):
    s3 = boto3.resource('s3')
    s3_object = s3.Object(Bucket=bucket_name, Key=filename)
    return s3_object.get()['Body'] #body returns streaming string


def write_s3_file(bucket_name, filename, file_data, **kwargs):
    file_type = kwargs.get("file_type", "json")
    if file_type == "json":
        file_to_write = bytes(json.dumps(file_data).encode("UTF-8"))
    elif file_type == "csv": # TODO
        with open(f"/tmp/{filename}.txt", 'w') as output_file:
            dict_writer = csv.DictWriter(output_file, file_data[0].keys())
            dict_writer.writeheader()
            dict_writer.writerows(file_data)
        file_to_write = open(f'/tmp/{filename}.txt', 'rb')

    if not filename.endswith(f".{file_type}"):
        filename = filename + f".{file_type}"

    try:
        s3_object = boto3.resource("s3").Object(bucket_name, filename)
        response = s3_object.put(Body=(file_to_write))
        status_code = ez_try_and_get(response, 'ResponseMetadata', 'HTTPStatusCode')
        if kwargs.get("enable_print"): logging.info(f"Successful write to {filename} / {status_code}")
        return status_code
    except Exception as e:
        logging.error(e, bucket_name, filename)


def get_s3_files_that_match_prefix(bucket_name, path, file_limit, **kwargs):
        s3_bucket = boto3.resource("s3").Bucket(bucket_name)

        output_list = []
        for n, file_summary in enumerate(s3_bucket.objects.filter(Prefix=path.lstrip("/")).limit(file_limit)):
            if kwargs.get('download_path'): # TODO does not work
                s3_bucket.download_file(file_summary.key, kwargs["download_path"])
            elif kwargs.get('return_names'):
                output_list.append(file_summary.key)
            else:
                file_dict = get_s3_file(bucket_name, file_summary.key, **kwargs)
                output_list.append({**file_dict, **{"s3_filename": file_summary.key}}) # add filename to the opened file's dict

        return output_list


# Only operates on one file at a time. Pair with get_s3_files_that_match_prefix and a for loop to copy a subfolder recursively
def copy_s3_file_to_different_bucket(start_bucket, start_path, dest_bucket, dest_path, **kwargs):
    destination_bucket = boto3.resource('s3').Bucket(dest_bucket)
    destination_bucket.copy({'Bucket': start_bucket, 'Key': start_path}, dest_path)

    if not kwargs.get("disable_print"):
        logging.info("Copy appears to have been a success")


# Only operates on one file at a time. Pair with get_s3_files_that_match_prefix and a for loop to copy a subfolder recursively
def move_s3_file(start_bucket, start_path, dest_bucket, dest_path, **kwargs):
    destination_bucket = boto3.resource('s3').Bucket(dest_bucket)
    destination_bucket.copy({'Bucket': start_bucket, 'Key': start_path}, dest_path)

    # Delete original after copying over
    delete_s3_file(start_bucket, start_path, disable_print=True)

    if not kwargs.get("disable_print"):
        logging.info(f"Move to {dest_path} appears to have been a success")


def move_s3_file_to_glacier(bucket_name, path):
    s3 = boto3.client('s3')

    s3.copy(
        {"Bucket": bucket_name, "Key": path},
        bucket_name,
        path,
        ExtraArgs={'StorageClass': 'GLACIER', 'MetadataDirective': 'COPY'}
    )
    return


def delete_s3_file(bucket_name, filename, **kwargs):
    try:
        response = boto3.resource("s3").Object(bucket_name, filename).delete()
        status_code = ez_try_and_get(response, 'ResponseMetadata', 'HTTPStatusCode')
        if not kwargs.get("disable_print"): logging.info(f"Successful delete of {filename} - Status Code: {status_code}")
        return status_code

    except ClientError as e:
        logging.error(e)
        return e


# Handles deleting abandoned delete markers, as well
def remove_s3_files_with_delete_markers(bucket_name, path, **kwargs):
    to_delete_dol = defaultdict(list)
    all_del_markers = {}

    bucket = boto3.resource('s3').Bucket(bucket_name)
    paginator = boto3.client('s3').get_paginator('list_object_versions')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=path) # , MaxKeys=kwargs.get("file_limit", None))

    for page in pages:
        if not page.get('DeleteMarkers'):
            continue

        # Get all delete markers where the *marker* is the latest version, i.e., should be deleted
        del_markers = {item['Key']: item['VersionId'] for item in page['DeleteMarkers'] if item['IsLatest'] == True}
        all_del_markers = {**all_del_markers, **del_markers}

        # Get all version IDs for all objects that have eligible delete markers
        for item in page.get('Versions', []):
            if item['Key'] in del_markers.keys():
                to_delete_dol[item['Key']].append(item['VersionId'])

    if kwargs.get("preview"):
        logging.info("NOTE: Just listing entries, not deleting.")
        [logging.info(f"{k} - {v}") for k,v in to_delete_dol.items()]
        return

    # Remove old versions of object by VersionId
    for del_item in to_delete_dol:
        if not kwargs.get("disable_print"):
            logging.info(f'Deleting {del_item}')
        object_to_remove = bucket.Object(del_item)

        for del_id in to_delete_dol[del_item]:
            object_to_remove.delete(VersionId=del_id)

        # Also remove delete marker itself
        object_to_remove.delete(VersionId=all_del_markers[del_item])



# http://ls.pwd.io/2013/06/parallel-s3-uploads-using-boto-and-threads-in-python/
# pass this a list of tuples of (filename, data)
def parallel_write_s3_files(bucket_name, file_lot):
    boto3.client('s3')
    for file_tuple in file_lot:
        t = threading.Thread(target = write_s3_file, args=(bucket_name, file_tuple[0], file_tuple[1])).start()

    logging.info(f"Parallel write to S3 Bucket {bucket_name} has commenced")

def parallel_delete_s3_files(bucket_name, file_list):
    boto3.client('s3')
    for filename in file_list:
        t = threading.Thread(target = delete_s3_file, args=(bucket_name, filename)).start()

    logging.info(f"Parallel delete to S3 Bucket {bucket_name} has commenced")



"""
Minimums for storage classes:
    Normal - None
    1Z Infrequent - 30 days
    Glacier - 90 days
"""



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


"""
   # TODO difference between
    s3 = boto3.resource("s3")
    s3.Object(bucket, filename)
    return obj.get()["Body"].read().decode("utf-8")
and
    s3 = boto3.client("s3")
    csv_file = s3.get_object(Bucket=bucket, Key=filename)["Body"]
"""


# DO NOTE: this will generate a url even if the file_name is not actually in the bucket
def generate_s3_presigned_url(bucket_name, file_name, **kwargs):
    client = boto3.client(
        's3',
        config=Config(
            signature_version='s3v4',
            s3 = {'use_accelerate_endpoint': kwargs.get("accelerate_endpoint", False)}
        )
    )

    url = client.generate_presigned_url(
        ClientMethod='get_object',
        Params={'Bucket': bucket_name, 'Key': file_name},
        ExpiresIn=kwargs.get("TTL", 3600) # one hour
    )
    return url


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

def convert_dict_to_parquet_map(input_dict, **kwargs):
    if not isinstance(input_dict, dict):
        logging.error(f"Malformed input to convert_dict_to_parquet_map: {input_dict}")
        return []

    output_list = []
    for k,v in input_dict.items():
        if kwargs.get("force_conversion") == "string":
            output_list.append( (str(k), str(v)) ) # tuple
        else:
            output_list.append( (k, v) ) # tuple
    return output_list

# only supports one day. If you have multiple dates in the data to be written, add it to the df/lod directly
def add_yearmonthday_partition_to_lod(data, partition_date):
    if partition_date in ["Today", "today", "utcnow", "", None]:
        partition_date = datetime.utcnow() # kwarg for if external oneoff file calling
    elif not isinstance(partition_date, datetime):
        logging.error("You must pass a datetime type value to add_yearmonthday_partition_to_lod")

    data['year'], data['month'], data['day'] = partition_date.year, partition_date.month, partition_date.day
    return data


def write_data_to_parquet_in_s3(data, s3_path, **kwargs):
    import pandas as pd
    import awswrangler as wr

    if isinstance(data, list) and isinstance(data[0], dict):
        data = pd.DataFrame(data)
    if not isinstance(data, pd.DataFrame):
        logging.error(f"Wrong type of data ({type(data)}) provided to write_data_to_parquet_in_s3")

    if kwargs.get("add_yearmonthday_partition"):
        data = add_yearmonthday_partition_to_lod(data, kwargs["add_yearmonthday_partition"])

    write_confirmation = wr.s3.to_parquet(
        df=data,
        path="s3://" + s3_path if not s3_path.startswith("s3://") else s3_path,
        dataset=True,                               # Stores as parquet dataset instead of 'ordinary file'
        index=False,                                # don't write save the df index
        sanitize_columns=True,                      # this happens by default
        mode=kwargs.get("mode", "append"),          # Could be append, overwrite or overwrite_partitions
        database=kwargs.get("database", None),      # Optional, only with you want it available on Athena/Glue Catalog
        table=kwargs.get("table", None),            # If not exists, it will create the table at the specific/s3/path you specify
        compression=kwargs.get("compression", "snappy"),
        max_rows_by_file=kwargs.get("max_rows_by_file", None), # If set = n, every n rows, split into a new file. If None, don't split
        partition_cols=kwargs.get("partition_cols", None),
        schema_evolution=kwargs.get("schema_evolution", False), # if True, and you pass a different schema, it will update the table
        concurrent_partitioning=False,
        use_threads=kwargs.get("use_threads", True),
        dtype=kwargs.get("dtype", None),
    )
    written_files = len(write_confirmation.get("paths", []))
    logging.info(f"Write was successful to path {s3_path}. There were {written_files} individual .pq files written")


def trigger_athena_table_crawl(s3_path, db, table, **kwargs):
    import pandas as pd
    import awswrangler as wr

    columns_types, partitions_types, partitions_values = wr.s3.store_parquet_metadata(
        path=s3_path,
        database=db,
        table=table,
        dataset=True,
        mode=kwargs.get("mode", "overwrite"),
        dtype=kwargs.get("col_dtype_dict", None) # dictionary of columns names and Athena/Glue types to be casted. Useful when you have columns with undetermined or mixed data types. (e.g. {'col name': 'bigint', 'col2 name': 'int'})
    )
    logging.info(f"Metadata crawl was successful of Athena table {table}")
    return columns_types, partitions_types, partitions_values


# when you read a year=2020, etc delimited data lake, the resulting df will have 'day', 'month', 'year' as columns
def read_s3_parquet(s3_path, **kwargs):
    import pandas as pd
    import awswrangler as wr

    if isinstance(s3_path, str):
        s3_path = "s3://" + s3_path if not s3_path.startswith("s3://") else s3_path

    df = wr.s3.read_parquet(
        path=s3_path,
        dataset=kwargs.pop("dataset", True),
        validate_schema=kwargs.pop("validate_schema", True), # raises an InvalidSchemaConvergence exception if > 1 schemas are found in the files
        use_threads=kwargs.pop("use_threads", True),
        ignore_empty=True,                                   # Ignore files with 0 bytes.
        # last_modified_begin=
        # last_modified_end=
        # columns=["only", "get", "these", "columns"]
    )
    return df

def extract_local_file_athena_metadata():
    """
    columns_types example: {'id': 'bigint', 'name': 'string', 'cost': 'double', 'event_date': 'date', 'updatedAt': 'timestamp'}
    partitions_types example: {'par0': 'bigint', 'par1': 'string'}

    :return:
    """
    import pandas as pd
    import awswrangler as wr

    columns_types, partitions_types = wr.catalog.extract_athena_types(
        df=df,
        file_format="csv",
        index=False,
        partition_cols=["par0", "par1"]
    )
    return columns_types, partitions_types


########################### ~ Glue Specific ~ ###################################################

"""
At present, this appears to only do exact string literal searches, which makes it near useless. I'm clearly missing something here.
Docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.search_tables
[ ] TODO: add support for NextToken
"""
def search_glue_tables(search_string, **kwargs):
    response = boto3.client('glue').search_tables(
        CatalogId=os.environ['AWS_ACCOUNT_ID'],
        SearchText=search_string,
        **kwargs
    )
    return response['TableList']


# col_lod entries look like [{'Name': 'col_name', 'Type': 'array<string>'},
def get_glue_table_columns(db, table, **kwargs):
    response = boto3.client('glue').get_table(
        CatalogId=os.environ['AWS_ACCOUNT_ID'],
        DatabaseName=db,
        Name=table
    )
    col_lod = ez_get(response, "Table", "StorageDescriptor", "Columns")
    if kwargs.get("return_type", "").lower() == "dict":
        return {x["Name"]:x["Type"] for x in col_lod}
    else:
        return [x['Name'] for x in col_lod]


def get_glue_table_location(db, table):
    response = boto3.client('glue').get_table(
        CatalogId=os.environ['AWS_ACCOUNT_ID'],
        DatabaseName=db,
        Name=table
    )
    table_location = response.get("Table", {}).get("StorageDescriptor", {}).get("Location", None)
    return table_location


def change_glue_table_s3_location(db, table, full_bucket_folder_path, **kwargs):
    change_location_sql_query = f"ALTER TABLE {db}.{table} "

    if kwargs.get("partition"):
        change_location_sql_query += kwargs['partition']  #  eg PARTITION (zip='98040', state='WA')
    if not full_bucket_folder_path.startswith("s3://"):
        full_bucket_folder_path = "s3://" + full_bucket_folder_path

    change_location_sql_query += f"SET LOCATION '{full_bucket_folder_path}';"
    query_athena_table(change_location_sql_query, db)
    logging.info(f"The {table} location change SQL query appears to have been successful")


# Dropping a glue table DOES NOT DELETE the underlying data; you have to do so separately
def drop_glue_table(db, table):

    drop_table_sql_query = f"DROP TABLE IF EXISTS `{db}.{table}`"
    query_athena_table(drop_table_sql_query, db)
    logging.info(f"The {table} drop appears to have been successful")


# NOT WORKING
def update_glue_table(db, table, **kwargs):
    table_input_dict = {
        'TargetTable': {
            'CatalogId': os.environ['AWS_ACCOUNT_ID'],
            'DatabaseName': db,
            'Name': table
        }
    }

    if not kwargs:
        raise ValueError("You must pass at least one kwarg")
    if kwargs.get("new_table_name"):
        table_input_dict['Name'] =  kwargs['new_table_name']
    if kwargs.get("new_table_location"):
        table_input_dict['StorageDescriptor']['Location'] =  kwargs['new_table_location']

    response = boto3.client('glue').update_table(
        CatalogId=os.environ['AWS_ACCOUNT_ID'],
        DatabaseName=db,
        TableInput=table_input_dict
    )
    print(response)
    return response


# the way the wrangler writes in main.py work we need to manually declare the day's partition daily
def add_glue_date_partition(db, table, bucket, subfolder_path, write_date, **kwargs):
    if not write_date:
        write_date = datetime.utcnow()

    update_partition_sql_query = f"""
    ALTER TABLE {db}.{table}
    ADD IF NOT EXISTS PARTITION (year={write_date.year}, month={write_date.month}, day={write_date.day}) 
    LOCATION '{bucket + subfolder_path}'
    """
    query_athena_table(update_partition_sql_query, db)


"""
The docs for this are just terrible - boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.delete_partition

To delete a partition like: 'account_id=abc123/year=2021/month=5/day=28', 
pass the following as partition_values_as_a_list: ["abc123", "2021", "5", 28"]

There is also batch_delete_partition, which TODO I'll implement here eventually
"""
def delete_glue_partition(db, table, partition_values_as_a_list):
    if not isinstance(partition_values_as_a_list, list) or not all(x for x in partition_values_as_a_list if isinstance(x, str)):
        logging.error(f"Double check your value for partition_values_as_a_list - {partition_values_as_a_list}")

    try:
        _ = boto3.client('glue').delete_partition(
            DatabaseName=db,
            TableName=table,
            PartitionValues=partition_values_as_a_list,
        )
    except Exception as e:
        if "EntityNotFoundException" in str(e):
            logging.warning(f"Specified glue partition not found - {e}")
        return False

    return True


########################### ~ CloudWatch Specific ~ ###################################################

# query = "fields @timestamp, @message | parse @message \"username: * ClinicID: * nodename: *\" as username, ClinicID, nodename | filter ClinicID = 7667 and username='simran+test@abc.com'"
# log_group = '/aws/lambda/NAME_OF_YOUR_LAMBDA_FUNCTION'
def query_cloudwatch_logs(query, log_group, lookback_hours, **kwargs):
    client = boto3.client('logs')
    params_dict = {
        "startTime": int((datetime.today() - timedelta(hours=lookback_hours)).timestamp()),
        "endTime": int(datetime.now().timestamp()),
        "queryString": query,
        "limit": kwargs.pop("limit", 1000),
    }
    if isinstance(log_group, str):
        params_dict["logGroupName"] = log_group
    elif isinstance(log_group, list):
        params_dict["logGroupNames"] = log_group

    start_query_response = boto3.client('logs').start_query(**params_dict)

    response = None
    while response == None or response['status'] == 'Running':
        sleep(0.25)
        response = client.get_query_results(
            queryId=start_query_response['queryId']
        )
    if kwargs.get("return_raw"):
        return response["results"]

    output_log_lod = []
    for log_row in response['results']:
        output_log_lod.append({x['field'].replace("@", ""):x['value'] for x in log_row})
        if not kwargs.get("keep_pointer"):
            output_log_lod[-1].pop("ptr", None)
        if not kwargs.get("keep_log_stream_prefix") and "message" in output_log_lod[-1]:
            output_log_lod[-1]['message'] = ez_split(output_log_lod[-1]['message'], "\t", -1)

    return output_log_lod


########################### ~ API Gateway Specific ~ ###################################################


def get_apiKey_usage(keyId, usagePlanId, **kwargs):
    today = datetime.utcnow()
    end_date = today + timedelta(days=int(kwargs.get("days_range", 1)))

    response = boto3.client('apigateway').get_usage(
        usagePlanId=usagePlanId,
        keyId=keyId,
        startDate=today.strftime("%Y-%m-%d"),
        endDate=end_date.strftime("%Y-%m-%d"),
    )
    return response.get("items", {})


def associate_api_gateway_key_with_usage_plan(key_id, plan_id):
    response = boto3.client('apigateway').create_usage_plan_key(
        usagePlanId=plan_id,
        keyId=key_id,
        keyType='API_KEY'
    )
    logging.info(f"Association of API Key id: {key_id} with Usage Plan id: {plan_id} had status_code: {ez_get(response, 'ResponseMetadata', 'HTTPStatusCode')}")
    return response

# You can't directly associate with an API Gateway Usage Plan at creation
def create_api_gateway_key(key_name, api_id, stage_name, **kwargs):
    response = boto3.client('apigateway').create_api_key(
        name=key_name,
        description=kwargs.get("description", f"Made via create_api_gateway_key at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}"),
        enabled=not kwargs.get("disabled", False),
        tags=kwargs.get("tags", {}),
        stageKeys=[{'restApiId': api_id, 'stageName': stage_name}],
    )
    logging.info(f"Creation of API Key id: {response.get('id')} had status_code: {ez_get(response, 'ResponseMetadata', 'HTTPStatusCode')}")

    if kwargs.get("plan_id"):
        _ = associate_api_gateway_key_with_usage_plan(response['id'], kwargs['plan_id'])

    return response




########################### ~ ECR Specific ~ ###################################################


def get_ecr_repo_image_digests(repo_name, **kwargs):
    kwargs = {k.replace("limit", "maxResults"):v for k,v in kwargs.items() if k in ["nextToken", "maxResults", "filter", "limit"]}
    response = boto3.client('ecr').list_images(
        registryId=os.environ['AWS_ACCOUNT_ID'],
        repositoryName=repo_name,
        **kwargs
    )

    logging.debug(ez_get(response, "ResponseMetadata", "HTTPStatusCode"))
    return response.get("imageIds", [])


# You can optionally pass a list of dictionaries of imageDigests imageIds=[{'imageDigest': 'string', 'imageTag': 'string'}]
def describe_ecr_repo_images(repo_name, **kwargs):
    kwargs = {k.replace("limit", "maxResults").replace("image_digest_lod", "imageIds"):v for k,v in kwargs.items() if k in ["image_digest_lod", "imageIds", "filter", "nextToken", "maxResults", "limit"]}
    response = boto3.client('ecr').describe_images(
        registryId=os.environ['AWS_ACCOUNT_ID'],
        repositoryName=repo_name,
        **kwargs
    )

    logging.info(f"Details were found for {len(response.get('imageDetails', []))} image(s). Status code: {ez_get(response, 'ResponseMetadata', 'HTTPStatusCode')}")
    return response.get("imageDetails", [])


########################### ~ SSM Parameter Store Specific ~ ###################################################


def get_ssm_param(param_name, **kwargs):
    ssm = boto3.client('ssm')
    try:
        result = ssm.get_parameter(Name=param_name, WithDecryption=True)
        return ez_try_and_get(result, 'Parameter', 'Value')
    except Exception as e: # e.g. ParameterNotFound throws an exception
        if not kwargs.get("disable_print") and "ParameterNotFound" in e:
            logging.error(e)

"""
SSM's Accepted kwargs: 
* Description
* Type='String'|'StringList'|'SecureString',
* KeyId
* Overwrite
* AllowedPattern
* Tags=[{'Key': 'string', 'Value': 'string'}]
* Tier='Standard'|'Advanced'|'Intelligent-Tiering',
* Policies='string',
* DataType='string'
"""
def put_ssm_param(param_name, param_value, param_type, **kwargs):
    if param_type not in ['String', 'StringList', 'SecureString']:
        raise ValueError("param_type must be one of ['String', 'StringList', 'SecureString']")

    ssm = boto3.client('ssm')
    result = ssm.put_parameter(Name=param_name, Value=param_value, Type=param_type, **kwargs)
    return ez_try_and_get(result, 'Parameter', 'Value')


########################### ~ STS Specific ~ ###################################################


def get_aws_account_id():
    response = boto3.client('sts').get_caller_identity()
    return response.get("Account")


########################### ~ Cognito Specific ~ ###################################################


def get_cognito_user_pool(pool_id):
    response = boto3.client('cognito-idp').describe_user_pool(UserPoolId=pool_id)
    return response['UserPool']


def create_cognito_user_pool(pool_config_dict):
    response =  boto3.client('cognito-idp').create_user_pool(**pool_config_dict)
    return response


# Not included in the GET, and not handled by this function: UserPoolAddOns
def duplicate_cognito_user_pool(initial_pool_id, new_name):
    existing_pool = get_cognito_user_pool(initial_pool_id)

    # Throw away keys specific to the original User Pool
    del existing_pool['AdminCreateUserConfig']['UnusedAccountValidityDays'] # weirdly the POST doesnt accept this; it inherits from ['Policies']['PasswordPolicy']['TemporaryPasswordValidityDays']
    del existing_pool['Arn']
    del existing_pool['CreationDate']
    del existing_pool['EstimatedNumberOfUsers']
    del existing_pool['Id']
    del existing_pool['LastModifiedDate']
    del existing_pool['Name']
    del existing_pool['SmsConfigurationFailure']

    # throw away default-but-not-wanted attributes
    attributes_to_keep = []
    for attribute in existing_pool.pop('SchemaAttributes'):
        if attribute.get("Name").startswith("custom:"):
            attribute['Name'] = startswith_replace(attribute['Name'], "custom:", "") # Cognito adds this custom: prefix behind the scenes
            attributes_to_keep.append(attribute)
        elif attribute.get("Required"):
            attributes_to_keep.append(attribute)

    existing_pool['Schema'] = attributes_to_keep
    existing_pool['PoolName'] = new_name

    response = create_cognito_user_pool(existing_pool)

    return response



