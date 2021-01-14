import os
import json
import re
from datetime import datetime, timedelta
import time
from functools import reduce
import logging
from collections import Counter
import timeit

try:
    import sentry_sdk
    from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
    sentry_kwargs = {"integrations": [AwsLambdaIntegration()]} if os.getenv("_HANDLER") else {}
    sentry_sdk.init(dsn=os.environ["SENTRY_DSN"], **sentry_kwargs)
except (ImportError, KeyError) as e:
    logging.warning(f"Sentry did not init: {e}")

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
    # if event.get("body"):  # POST, synchronous API Gateway TODO
    #     event.update(event["body"])
    if event.get("queryStringParameters"):  # GET, synchronous API Gateway
        event.update(event["queryStringParameters"])
    if event.get("query"):  # GET, async API Gateway
        event.update(event["query"])
    if event.get("Records"):  # triggered directly by SQS queue TODO only first record?
        event.update(json.loads(ez_try_and_get(event, "Records", 0, "body")))


    return standardize_dict(event)


# Necessary for API Gateway to return
def package_response(message, status_code, **kwargs):
    if kwargs.get("log"):
        logging.info(message)
    elif kwargs.get("warn"):
        logging.warning(message)
    elif kwargs.get("error"):
        logging.error(message)

    return {
        'statusCode': status_code if status_code else '200',
        'body': json.dumps({'data': message}) if not kwargs.get("message_as_key") else json.dumps({'message': message}),
        'headers': {'Content-Type': 'application/json'}
    }

class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        # if isinstance(obj, complex):
        #     return [obj.real, obj.imag]
        if isinstance(obj, numpy.int64):
            return int(obj)
        if isinstance(obj, set):
            return list(obj)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def invoke_lambda(params, function_name, invoke_type):

    lambda_client = boto3.client("lambda", region_name="us-west-1")
    lambda_response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType=invoke_type,
        Payload=json.dumps(params), # default=ComplexEncoder TODO
    )
    # Async Invoke (Event) returns only StatusCode
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


def get_apiKey_usage(keyId, usagePlanId, **kwargs):
    today = datetime.utcnow()
    tomorrow = today + timedelta(days=int(kwargs.get("days_range", 1)))

    client = boto3.client('apigateway')
    response = client.get_usage(
        usagePlanId=usagePlanId,
        keyId=keyId,
        startDate=today.strftime("%Y-%m-%d"),
        endDate=tomorrow.strftime("%Y-%m-%d"),
    )
    return response.get("items", {})

######################### ~ General  Formatting ~ ########################################################


# This corrects for an edge case where some None values may convert to str "None" by API Gateway
def standardize_dict(input_dict):
    return {k.title().strip().replace(" ", "_"):(False if is_none(v) else v) for (k, v) in input_dict.items()}


# can this be deprecated? TODO
def standardize_str_to_list(input_str):
    if isinstance(input_str, list):
        return input_str
    output_list = input_str.split(", ")
    output_list = [x for x in output_list if x] # Drop falseys
    output_list = [x.strip() if isinstance(x, str) else x for x in output_list]
    return output_list


def get_list_overlap(list_1, list_2, **kwargs):
    if kwargs.get("case_insensitive"): # will keep item from 1st list

        list_2 = [x.lower() if isinstance(x, str) else x for x in list_2]
        output_list = []
        for item in list_1:
            if isinstance(item, str) and item.lower().strip() in list_2:
                output_list.append(item)
            elif item in list_2:
                output_list.append(item)
        return output_list

    return list(set(list_1).intersection(list_2))


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
    elif isinstance(phrase, list) or isinstance(phrase, set):
        return delimiter.join(str(v) for v in phrase)
    elif isinstance(phrase, str):
        return phrase
    elif type(phrase) == 'numpy.ndarray':
        return delimiter.join(list(phrase))
    else:
        return phrase


def ez_split(phrase, delimiter, return_slice, **kwargs):
    if not (phrase and delimiter and delimiter in phrase):
        return kwargs.get("fallback_value", phrase)

    if type(return_slice) != type(True) and isinstance(return_slice, int):
        return phrase.split(delimiter)[return_slice]
    else:
        return [x.strip() for x in phrase.split(delimiter)]


# there's no re.find. I named this _find because _match makes more semantic sense than _search, but the .search operator is more useful than the .match operator
# Note: keep in mind 0-indexing when using group=1, etc. group=1 is the second group.
def ez_re_find(pattern, text, **kwargs):
    if isinstance(text, list) or isinstance(text, set):
        text = ez_join(text, " ")

    if kwargs.get("find_all"):
        return set([x.groups() for x in re.finditer(pattern, text)])

    possible_match = re.search(pattern, text)
    if not possible_match:
        return ""
    elif kwargs.get("group") and isinstance(kwargs["group"], int):
        return possible_match.groups()[kwargs["group"]]
    else:
        return possible_match.group() # if possible_match else ""


# Print/log to the terminal in color!
def colored_log(log_level, text, color):
    color_dict = {
        'White': '\033[39m',
        'Red': '\033[31m',
        'Blue': '\033[34m',
        "Cyan": '\033[36m',
        "Bold": '\033[1m',
        'Green': '\033[32m',
        'Orange': '\033[33m',
        'Magenta': '\033[35m',
        'Red_Background': '\033[41m',
    }
    # Trailing white prevents the color from staying applied
    message = color_dict[color.title()] + text + color_dict[color.title()] + color_dict['White']

    if log_level.lower() == "info":
        logger.info(message)
    elif log_level.lower() in ["warn", "warning"]:
        logger.warn(message)
    elif log_level.lower() == "error":
        logger.error(message)


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


################################################ ~ URL string handling ~ ######################################################################

"""
Note: this will return false positives for made up TLDs that contain viable TLDs
ex: '.ae.com' is a true positive TLD, but the made up '.aee.com' is false positive, as it contains '.com'
This shouldn't be a problem if your data isn't extremely dirty
"""
def is_url(potential_url_str, **kwargs):
    tld_list = kwargs.get("tld_list", get_tld_list())
    if find_substrings_in_string(potential_url_str, tld_list):
        return True

    return False


def is_ipv4(potential_ip_str):
    pieces = potential_ip_str.split('.')
    if len(pieces) != 4: is_ip = False
    try: is_ip = all(0<=int(p)<256 for p in pieces)
    except ValueError: is_ip = False
    logging.debug(f"String {potential_ip_str} is_ipv4: {is_ip}")
    return is_ip


"""
Keep in mind removals stack - e.g. remove_tld will remove subsite, port, and trailing slash
for kwargs remove_tld and remove_subdomain, you can fetch tld_list ahead of time and pass it in to save 1ms per
Known problem: strings like "lunarcovers.co.ukasdfij" will match .co.uk and return as 'lunarcovers.co.uk'
"""
def format_url(url, **kwargs):
    if not url:
        return url
    # if kwargs.get("check_if_ipv4") and is_ipv4(url): # TODO
    #     return url

    url = ez_split(url, "://", 1)
    url = ez_split(url, "www.", 1)

    if kwargs.get("remove_subsite"):
        url = ez_split(url, "/", 0)
    if kwargs.get("remove_tld"):
        url = ez_split(url, find_url_tld(url, kwargs["remove_tld"]), 0)
    if kwargs.get("remove_port") and ":" in url:
        pattern = re.compile("(:\d{2,})")
        url = pattern.sub('', url)
    if kwargs.get("remove_trailing_slash"):
        url = url.rstrip("/")
    if kwargs.get("remove_subdomain") and url.count(".") > 1:
        tld = find_url_tld(url, kwargs["remove_subdomain"])
        if not tld:
            return url.strip()
        subdomain = ez_split(url, tld, 0, fallback_value="")
        domain = subdomain[subdomain.rfind(".")+1:]
        url = domain + tld

    if kwargs.get("https"):
        url = "https://" + url
    elif kwargs.get("http"):
        url = "http://" + url

    return url.strip().rstrip("\\")


def find_url_tld(url, tld_list):
    tld_list = tld_list if isinstance(tld_list, list) else get_tld_list()
    matched_tlds = find_substrings_in_string(url, tld_list)
    if not matched_tlds:
        logging.warning(f"No TLD in {url}")
        return None
    tld = max(matched_tlds, key=len) # get the longest matching string TLD
    return tld


def get_tld_list():
    # from https://tld-list.com/tlds-from-a-z
    try:
        with open("utility/TLD_list.txt") as f:
            return [line.rstrip() for line in f]
        # lines = tuple(open("utility/TLD_list.txt", 'r'))
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Make sure your utility submodule folder is called utility. {e}")


############################################# ~ Datetime/str handling ~ ##########################################################


# TODO deprecate
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


def detect_and_convert_datetime_str(datetime_str, **kwargs):
    if not datetime_str:
        return kwargs.get("null_value", "")

    if datetime_str.isdigit() and len(datetime_str) in [9, 10]: # assume UTC
        output_dt = datetime.utcfromtimestamp(int(datetime_str))
        return datetime.strftime(output_dt, kwargs.get("output_format", "%Y-%m-%d %H:%M:%S"))
    
    LIST_OF_DT_FORMATS = ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S %Z", "%Y-%m-%d %H:%M:%ST%z", "%Y-%m-%d %H:%M:%S %z %Z", "%Y-%m-%dT%H:%M:%S%z", "%a, %d %b %Y %H:%M:%S %Z", "%a %b %d, %Y", "%m/%d/%Y %H:%M:%S %p", "%A, %B %d, %Y, %H:%M %p",  "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S.SSSZ", "%a %b %d %Y %H:%M:%S %Z%z", "%Y-%m-%d"]
    for dt_format in LIST_OF_DT_FORMATS:
        try:
            dt_str = datetime.strptime(datetime_str, dt_format)
            standard_dt_str = datetime.utctimetuple(dt_str) # convert to UTC
            break
        except:
            if dt_format == LIST_OF_DT_FORMATS[-1]: # if none matched
                logging.warning(f"The datetime_str {datetime_str} did not match any pattern")
                return kwargs.get("null_value", "") # returns empty str by default

    try:
        output_dt = datetime.utcfromtimestamp(time.mktime(standard_dt_str)) # convert from time.struct_time to datetime.date
        return datetime.strftime(output_dt, kwargs.get("output_format", "%Y-%m-%d %H:%M:%S"))
    except:
        return kwargs.get("null_value", "")


############################################# ~ List/dict handling ~ ##########################################################


# It's faster if you have a primary_key in each dict
def deduplicate_lod(input_lod, primary_key):

    # convert to JSON to make dicts hashable then add to a set to dedupe
    if not primary_key:
        output_los = {json.dumps(d, sort_keys=True) for d in input_lod}
        return [json.loads(d) for d in output_los]

    # for each input dict, check if the value of the dict's primary_key is in the output already, if so, write the dict to the output.value
    output_dict = {}
    for d in input_lod:
        if d.get(primary_key) not in output_dict.keys():
            output_dict[d[primary_key]] = d

    return list(output_dict.values())


# e.g. checking if any tld exists in a string
def find_substrings_in_string(value, list_of_substrings, **kwargs):
    if not value or not list_of_substrings:
        logging.debug("One of value or list_of_substrings was None in find_substrings_in_string")
        return []

    if kwargs.get("no_strip"): # no whitespace strip
        return [sub_str for sub_str in list_of_substrings if sub_str.lower() in value.lower()]
    return [sub_str for sub_str in list_of_substrings if sub_str.lower().strip() in value.lower().strip()]


# i.e. split a list of len n into x smaller lists of len (n/x)
def split_list_to_fixed_length_lol(full_list, subsection_size):
    if not len(full_list) > subsection_size:
        return [full_list] # Return list as LoL
    return [full_list[i:i+subsection_size] for i in range(0, len(full_list), subsection_size)]


def combine_lists_unique_values(*args):
    output_set = set()
    for input_list in args:
        for item in input_list:
            output_set.add(item)
    return list(output_set)


def increment_counter(counter, *args, **kwargs):
    if len(args) == 1 and isinstance(args[0], list):
        args = args[0]
    for arg in args:
        counter[arg] += 1

    for arg_to_del in kwargs.get("del_keys", []):
        del counter[arg_to_del]

    return counter
