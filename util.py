import os
import json
from datetime import datetime, timedelta
from functools import reduce
import logging

try:
    import sentry_sdk
    from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
    sentry_sdk.init(
        dsn=os.environ["SENTRY_DSN"],
        integrations=[AwsLambdaIntegration()]
    )
except ImportError:
    logging.warning("Sentry did not init")

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# Allows enforcing of querystrings' presence
def validate_params(event, required_params, **kwargs):
    event = standardize_event(event)
    commom_required_params = get_list_overlap(event, required_params)
    commom_optional_params = get_list_overlap(event, kwargs.get("optional_params", []))

    param_only_dict = {k:v for k, v in event.items() if k in required_params+kwargs.get("optional_params", [])}
    logging.info(f"Total param dict: {param_only_dict}")
    logging.info(f"Found optional params: {commom_optional_params}")

    if commom_required_params != required_params:
        missing_params = [x for x in required_params if x not in event]
        return param_only_dict, missing_params

    return param_only_dict, False

# unpack the k:v pairs into the top level dict. Standard across invoke types.
def standardize_event(event):
    # if event.get("body"):  # POST, synchronous API Gateawy TODO
    #     event.update(event["body"])
    if event.get("queryStringParameters"):  # GET, synchronous API Gateway
        event.update(event["queryStringParameters"])
    if event.get("query"):  # GET, async API Gateway
        event.update(event["query"])
    if event.get("Records"):  # triggered directly by SQS queue
        event.update(json.loads(ez_try_and_get(event, "Records", 0, "body")))


    return standardize_dict(event)


# Necessary for API Gateway to return
def package_response(message, status_code, **kwargs):
    if kwargs.get("log"):
        logging.info(message)
    elif kwargs.get("warn"):
        logging.warning(message)

    return {
        'statusCode': status_code if status_code else '200',
        'body': json.dumps({'data': message}),
        'headers': {'Content-Type': 'application/json'}
    }


def invoke_lambda(params, function_name, invoke_type):

    lambda_client = boto3.client("lambda")
    lambda_response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType=invoke_type,
        Payload=json.dumps(params),
    )
    # Async Invoke returns only StatusCode
    if invoke_type.title() == "Event":
        return None, lambda_response.get("StatusCode", 666)

    string_response = lambda_response["Payload"].read().decode("utf-8")
    json_response = json.loads(string_response)

    if not json_response:
        return "Unknown error: no json_response. Called lambda may have timed out.", 500
    elif json_response.get("errorMessage"):
        return json_response.get("errorMessage"), 500

    status_code = int(json_response.get("statusCode"))
    json_body = json.loads(json_response.get("body"))

    if json_response.get("body") and json_body.get("error"):
        return json_body.get("error").get("message"), status_code

    return json_body["data"], status_code


######################### ~ General str/list Formatting ~ ########################################################

# This corrects for an edge case where some None values may convert to str "None" by API Gateway
def standardize_dict(input_dict):
    return {k.title().strip().replace(" ", "_"):(False if is_none(v) else v) for (k, v) in input_dict.items()}


def standardize_str_to_list(input_str):
    if isinstance(input_str, list):
        return input_str
    output_list = input_str.split(", ")
    output_list = [x for x in output_list if x] # Drop falseys
    output_list = [x.strip() if isinstance(x, str) else x for x in output_list]
    return output_list


def get_list_overlap(list_1, list2):
    return list(set(list_1).intersection(list2))


def ez_get(nested_data, *keys):
    return reduce(lambda d, key: d.get(key) if d else None, keys, nested_data)


def ez_try_and_get(nested_data, *keys):
    for key in keys:
        try:
            nested_data = nested_data[key]
        except (KeyError, TypeError, AttributeError, IndexError):
            return None
    return nested_data


def ez_join(phrase, delimiter):
    if is_none(phrase):
        return ""
    elif isinstance(phrase, str):
        return phrase
    elif isinstance(phrase, list):
        return delimiter.join(str(v) for v in phrase)
    elif type(phrase) == 'numpy.ndarray':
        return delimiter.join(list(phrase))
    else:
        return phrase


def ez_split(phrase, delimiter, return_slice):

    if not (phrase and delimiter in phrase):
        return phrase

    if type(return_slice) != type(True) and isinstance(return_slice, int):
        return phrase.split(delimiter)[return_slice]
    else:
        output_list = phrase.split(delimiter)
        return [x.strip() for x in output_list]


def is_lod(possible_lod):
    return all(isinstance(el, dict) for el in possible_lod)


def is_none(value, **kwargs):
    None_List = ['None', 'none', 'False', 'false', 'No', 'no', None, False, ["None"], ["False"]]

    if kwargs.get("keep_0") and value is 0:
        return False
    if not value:
        return True
    elif isinstance(value, str) and value in None_List:
        return True

    return False


def is_url(value):
    tld_list = ['.de', '.com', '.info', '.es', '.mil', '.no', '.vc', '.au', '.se', '.io', '.tv', '.co', '.fr', '.uk', '.ai', '.ch', '.org', '.ca', '.gov', '.ly', '.net', '.ru', '.nl', '.us', '.it', '.jp', '.edu', '.biz', '.xml', '.ph', '.id', '.tw', '.hk', '.ro', '.eu', '.in', '.by', '.mx', '.cz', '.dk', '.si', '.solutions', '.fi', '.life', '.city', '.ie', '.br', '.pk', '.be', '.ae', '.pl', '.do', '.earth', '.lt', '.pt', '.cl', '.br', '.cd', '.uz', '.nu', '.cn', '.at', '.fm', '.ir', '.nz', '.trading', '.mn', '.wales']
    if any(tld for tld in tld_list if tld.lower().strip() in value.lower().strip()):
        return True

    return False


def format_timestamp(timestamp, **kwargs):
    if not timestamp:
        return None, None

    try:
        timestamp = datetime.utcfromtimestamp(float(timestamp))
    except ValueError as e:
        try:
            timestamp = datetime.strptime(timestamp, kwargs.get("input_format",'%Y-%m-%dT%H:%M:%SZ'))
        except ValueError as e2:
            return None, timestamp

    if not timestamp:
        return None, timestamp

    try:
        timestamp = timestamp + timedelta(hours=-8)                      # PDT. Note PST is 8 hours
        timestamp_str = datetime.strftime(timestamp, kwargs.get("output_format", '%Y-%m-%d %H:%M:%S'))
    except OverflowError:
        logging.error(timestamp, "overflow on timedelta -8")
        return None, None

    return timestamp_str, timestamp


def format_url(url, **kwargs):
    url = ez_split(url, "://", 1)
    url = ez_split(url, "www.", 1)

    if kwargs.get("remove_subsite"):
        url = ez_split(url, "/", 0) # maybe rfind instead TODO
    if kwargs.get("remove_tld"):
        url = url[:url.rfind(".")]
    if kwargs.get("remove_port"):
        url = url.replace(":80", "")
    if kwargs.get("https"):
        url = "https://" + url
    if kwargs.get("remove_trailing_slash") and url.endswith("/"):
        url = ez_split(url, "/", 0)

    return url.rstrip()


# It's faster if you have a primary_key in each dict
def deduplicate_lod(input_lod, primary_key):
    if not primary_key:
        output_lod = {json.dumps(d, sort_keys=True) for d in input_lod}  # convert to JSON to make dicts hashable
        return [json.loads(x) for x in output_lod]                 # unpack the JSON

    output_dict = {}
    for d in input_lod:
        if d.get(primary_key) not in output_dict.keys():
            output_dict[d[primary_key]] = d

    return list(output_dict.values())

# e.g. checking if any tld exists in a string
def find_substrings_in_string(value, list_of_substrings):
    return [sub_str for sub_str in list_of_substrings if sub_str.lower().strip() in value.lower().strip()]

# e.g. split a list of len n into x smaller lists of len (n/x)
def split_list_to_fixed_length_lol(full_list, subsection_size):
    if not len(full_list) > subsection_size:
        return [full_list] # Return list as LoL
    return [full_list[i:i+subsection_size] for i in range(0, len(full_list), subsection_size)]
