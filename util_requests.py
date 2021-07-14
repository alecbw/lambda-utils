from utility.util import package_response, standardize_event, validate_params, format_url, fix_JSON
from utility.util_datastores import scan_dynamodb

import random
import logging
import os
from time import sleep
import warnings
import json
import re

from bs4 import BeautifulSoup, element, NavigableString
import requests
from urllib3.packages.ssl_match_hostname import CertificateError
from urllib3.exceptions import MaxRetryError, ProtocolError
from requests.exceptions import ProxyError, ConnectionError, HTTPError, SSLError, Timeout, TooManyRedirects


def api_request(url, request_type, **kwargs):
    verbose_print = kwargs.pop("print", False)
    raw_response = kwargs.pop("raw_response", False)

    if kwargs.get("bearer_key"):
        kwargs["headers"] = {"Authorization": "Bearer " + kwargs.pop("bearer_key")}
    elif kwargs.get("header_key"):
        kwargs["headers"] = {"x-api-key": kwargs.pop("header_key")}

    request_dict = {
        "GET": requests.get(url.strip(), **kwargs),
        "POST": requests.post(url.strip(), **kwargs),
        "DELETE": requests.delete(url.strip(), **kwargs),
    }
    response = request_dict[request_type.upper()]

    if verbose_print and response.status_code not in [200, 202]:
        logging.warning(response.status_code)
        logging.warning(response.url)
        logging.warning(response.headers)
        logging.warning(response.content)

    if raw_response:
        return response
    elif response.status_code in [200, 202]:
        return response.json()
    else:
        return False



################################# ~ Header Modification ~ ####################################


# Mock a series of different browser / OS types
def rotate_agent():
    agents = ["Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36",           # Desktop
              "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/601.2.7 (KHTML, like Gecko) Version/9.0.1 Safari/601.2.7",   # Desktop
              "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36",          # Desktop
              "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0.1 Safari/602.2.14",
              "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36",
              "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36",
              "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36",
              "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36",
              "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36",
              "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
              "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0",
              "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36",
              "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/601.1.56 (KHTML, like Gecko) Version/9.0 Safari/601.1.56",
              "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36",
              "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36",
              "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.11 Safari/534.16",
              "Mozilla/5.0 (Windows; Windows NT 6.1; rv:2.0b2) Gecko/20100720 Firefox/4.0b2",
              "Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36",
              "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36",
              "Mozilla/5.0 (X11; Linux x86_64; rv:2.0b4) Gecko/20100818 Firefox/4.0b4",
              "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:57.0) Gecko/20100101 Firefox/57.0",
              "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
              "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0;  Trident/5.0)",
              "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.45 Safari/535.19",
              "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.5; ko; rv:1.9.1b2) Gecko/20081201 Firefox/3.1b2",
              "Mozilla/5.0 (Macintosh; U; Intel Mac OS X; en-US) AppleWebKit/533.4 (KHTML, like Gecko) Chrome/5.0.375.86 Safari/533.4",
              "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.8; rv:25.0) Gecko/20100101 Firefox/25.0",
              "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.38 Safari/537.36",
              "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36",
              "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko",
              "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 Edge/16.16299",
              "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36",
              "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
              "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:58.0) Gecko/20100101 Firefox/58.0",
              "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.2 (KHTML, like Gecko) Chrome/15.0.872.0 Safari/535.2",
              "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36",
              "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36",
              "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36", # recent chrome
              "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.106 Mobile Safari/537.36",
              "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36", # # 1 Browser: Chrome 68.0 Win10 16-bit
              "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36", # # 2 Browser: Chrome 69.0 Win10 16-bit
              "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36"] # # 3 Browser: Chrome 68.0 macOS 16-bit
    return random.choice(agents)


# Mock the refering domain. The mulitple occurance of certain search engines reflects their relative popularity
def rotate_referer():
    referers = ["www.bing.com",
                "www.yahoo.com",
                "www.google.com", "www.google.com", "www.google.com", "www.google.com"
                "www.duckduckgo.com"]
    return random.choice(referers)


def rotate_encoding():
    encodings = ["gzip, deflate, sdch", "gzip, deflate"] #"gzip, deflate, br, sdch", "gzip, deflate, br"
    return random.choice(encodings)


def rotate_language():
    languages = ["en-US,en;q=0.8", "en-US,en;q=0.9"]
    return random.choice(languages)


def rotate_accept():
    accepts = ["text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9"]
    return random.choice(accepts)


################################# ~ Proxies ~ ####################################


def get_ds_proxy_list(**kwargs):
    countries = kwargs.get("countries", "US|CA|MX|AT|BE|HR|CZ|DK|EE|FL|FR|DE|GB|GR|HU|IE|IT|LU|LT|LI|MC|NL|NO|PL|RO|RS|CS|SK|SI|ES|SE|CH|GB")
    url = os.environ["DS_URL"] + f"&showcountry={kwargs.get('show_country', 'no')}&country={countries}&https={kwargs.get('HTTPS', 'yes')}"
    url += "&level=1|2"

    response = api_request(url, "GET", raw_response=True)
    proxies = [x.decode("utf-8") for x in response.iter_lines()] # bc it returns raw text w/ newlines
    logging.info(f"{len(proxies)} proxies were found (DS)")
    if kwargs.get('show_country'):
        return [x.split("#") for x in proxies]

    return proxies

# def rotate_ds_proxy(proxies):
#     if len(proxies) == 0:
#         logging.info("Exhausted list; getting another")
#         proxies = get_ds_proxy_list()
#
#     proxy = proxies.pop(0)
#     return proxy, proxies


def rotate_proxy(proxies, **kwargs):
    if not proxies:
        proxies =  prioritize_proxy(scan_dynamodb('proxyTable'), "US")

    if kwargs.get("return_proxy_dict"):
        return proxies.pop(0), proxies

    return proxies.pop(0).get("full"), proxies


# Sorts the list of proxies by location so the specified locations' proxies are first
def prioritize_proxy(proxies, location):
    output_proxies_list = []
    for proxy in proxies:
        if proxy.get("location") == location:
            output_proxies_list.insert(0, proxy)
        else:
            output_proxies_list.append(proxy)
    return output_proxies_list


################################# ~ Outbound Requests ~ ####################################


def handle_request_exception(e, disable_error_messages):
    if "Caused by SSLError(SSLCertVerificationError" in str(e): # CertificateError
        warning = f'-----> ERROR. Request Threw: Certificate Error. {e}<-----'
        status_code = 495
    elif "Exceeded 30 redirects" in str(e):
        warning = f'-----> ERROR. Request Threw: Too Many Redirects Error. {e}<-----'
        status_code = 399
    elif "TimeoutError" in str(e) or " Read timed out." in str(e):
        warning = f'-----> ERROR. ROTATE YOUR PROXY. Request Threw TimeoutError: {e} <-----'
        status_code = 408
    elif "Caused by NewConnectionError" in str(e) and "ProxyError" not in str(e):
        warning = f'-----> ERROR. ROTATE YOUR PROXY. Effective 404 - Request Threw NewConnectionError: {e} <-----'
        status_code = 404
    elif "Connection refused" in str(e): # or "Remote end closed connection" in str(e):
        warning = f'-----> ERROR. ROTATE YOUR PROXY. Proxy refusing traffic {e} <-----'
        status_code = 602
    elif any(x for x in ["HTTPConnectionPool", "MaxRetryError" "ProxyError", "SSLError", "ProtocolError", "ConnectionError", "HTTPError", "Timeout"] if x in str(e)):
        warning = f'-----> ERROR. ROTATE YOUR PROXY. {e}<-----'
        status_code = 601
    else:
        warning = f'-----> ERROR. Request Threw: Unknown Error. {e}<-----'
        logging.warning(warning)
        status_code = 609

    if not disable_error_messages:
        logging.warning(warning)

    return warning, status_code


# Mock a browser and visit a site
def site_request(url, proxy, wait, **kwargs):
    if kwargs.get("disable_error_messages"):
        # warnings.simplefilter('ignore', SSLError)
        logging.getLogger("requests").setLevel(logging.ERROR)
        logging.getLogger("urllib3").setLevel(logging.ERROR)

    if wait and wait != 0:
        sleep(random.uniform(wait, wait+1))    # +/- 0.5 sec from specified wait time. Pseudorandomized.

    if kwargs.get("clean_url"):
        url = url.split("://", 1)[1] if "://" in url else url
        url = url.split("www.", 1)[1] if "www." in url else url
        url = "https://" + url

    # Spoof a typical browser header. HTTP Headers are case-insensitive.
    headers = {
        'user-agent': kwargs.get("agent", rotate_agent()),
        'referer': kwargs.get("referer", rotate_referer()),          # Note: this is intentionally a misspelling of referrer
        'accept-encoding': rotate_encoding(),
        'accept-language': rotate_language(),
        'accept': rotate_accept(),
        'cache-control': "no-cache",
        'DNT': "1",                                              # Ask the server to not be tracked (lol)
    }
    if kwargs.get("no_headers"):
        headers = {}
    if not kwargs.get("http_proxy"):
        headers['upgrade-insecure-requests'] = "1"  # Allow redirects from HTTP -> HTTPS

    try:
        approved_request_kwargs = ["prevent_redirects", "timeout", "hooks"]
        request_kwargs = {k:v for k,v in kwargs.items() if k in approved_request_kwargs}
        request_kwargs["allow_redirects"] = False if request_kwargs.pop("prevent_redirects", None) else True # TODO refactor this out

        if kwargs.get("http_proxy"):
            request_kwargs["proxies"] = {"http": f"http://{proxy}"}
        elif proxy:
            request_kwargs["proxies"] = {"http": f"http://{proxy}", "https": f"https://{proxy}"}

        logging.debug(f"Now requesting {url}")
        response = requests.get(url, headers=headers, **request_kwargs)

    except Exception as e:
        message, applied_status_code = handle_request_exception(e, kwargs.get("disable_error_messages"))
        return message, applied_status_code


    if response.status_code in [502, 503, 999] and not kwargs.get("disable_error_messages"):
        logging.warning(f'-----> ERROR. Request Threw: {response.status_code}. ROTATE YOUR PROXY <-----')
    elif response.status_code not in [200, 202, 301, 302] and not kwargs.get("disable_error_messages"):
        logging.warning(f'-----> ERROR. Request Threw: {response.status_code} <-----')

    if kwargs.get("soup"):                       # Allow functions to specify if they want parsed soup or plain request resopnse
        return BeautifulSoup(response.content, 'html.parser'), response.status_code
    else:
        return response, response.status_code


# This will handle 1) Fetching and Rotating proxies and 2) Handling and Retrying failed requests
def iterative_managed_site_request(url_list, **kwargs):
    proxies = scan_dynamodb('proxyTable', Limit=kwargs.get("proxies", 5))
    proxy = proxies.pop(0).get("full")

    result_list = []
    for url in url_list:
        response, status_code = site_request(url, proxy, kwargs.pop("wait", 1), **kwargs)

        while isinstance(response, str): # Returned error messaged
            if "SSLError" in response:
                logging.info("Trying without HTTPS")
                url = url.replace("https", "http")
            proxy = proxies.pop(0).get("full")
            response, status_code = site_request(url, proxy, wait=kwargs.pop("wait", 1), **kwargs)

        result_list.append([response, status_code])

    return result_list


############################## ~ Handling HTML ~ ####################################

def ez_strip_str(input_str, **kwargs):
    if not isinstance(input_str, str):
        logging.warning(f"non str fed to ez_strip_str {input_str}")
        return input_str
    elif not ez_strip_str:
        return input_str

    if kwargs.get("reduce_interior_whitespace"):
        input_str = re.sub(r"\s{2,}", " ", input_str)
    return input_str.replace(" \n", "").replace(" \r", "").replace("\n ", "").replace("\r ", "").replace("\n", " ").replace("\r", " ").replace('\\xa0', ' ').replace(r"\xa0", " ").replace(u'\xa0', ' ').replace("&amp;", "&").replace("&#039;", "'").replace("&#8211;", "-").replace("&nbsp", " ").replace("•", " ").replace("%20", " ").replace(r"\ufeff", " ").replace(" &ndash;", " -").replace("u0022", '"').strip()

# TODO replace dumbass implementation of replacing newline chars
def extract_stripped_string(html_tag_or_str, **kwargs):
    if not html_tag_or_str:
        return kwargs.get("null_value", html_tag_or_str)

    elif isinstance(html_tag_or_str, NavigableString) and str(html_tag_or_str):
        return ez_strip_str(str(html_tag_or_str))#.replace(" \n", "").replace(" \r", "").replace("\n ", "").replace("\r ", "").replace("\n", " ").replace("\r", " ").replace('\\xa0', ' ').replace(r"\xa0", " ").replace(u'\xa0', ' ').strip()

    elif isinstance(html_tag_or_str, str):
        return ez_strip_str(html_tag_or_str)

    elif html_tag_or_str.get_text():
        return ez_strip_str(html_tag_or_str.get_text(separator=kwargs.get("text_sep", " "), strip=True))#.replace(" \n", "").replace(" \r", "").replace("\n ", "").replace("\r ", "").replace("\n", " ").replace("\r", " ").replace('\\xa0', ' ').replace(r"\xa0", " ").replace(u'\xa0', ' ')

    return kwargs.get("null_value", html_tag_or_str)


def get_script_json_by_contained_phrase(parsed, phrase_str, **kwargs):
    find_all_kwargs = {k:v for k,v in kwargs.items() if k in ["id", "href", "attrs", "type", "name", "property"]}
    for script in parsed.find_all('script', **find_all_kwargs):
        if script and script.string and phrase_str in script.string:
            if kwargs.get("lstrip"):
                script.string = script.string.lstrip(kwargs['lstrip'])
            if kwargs.get("return_string"):
                return script.string.strip().rstrip(",")

            while '“' in script.string or '”' in script.string:
                char_index = script.string.find('”') if script.string.find('”') != -1 else script.string.find('”')
                if ":" in script.string[char_index-2:char_index+3] or "," in script.string[char_index-2:char_index+3]:
                    script.string = replace_string_char_by_index(script.string, char_index, '"')
                else:
                    script.string = replace_string_char_by_index(script.string, char_index, r'\"')
                # script.string =  script.string.replace(':“', ':"').replace(':”', ':"').replace(': “', ': "').replace(': ”', ': "').replace('“,', '",').replace('”,', '",') # beginning or end of
                # script.string =  script.string.replace('“', r'\"').replace('”', r'\"') # interior quotation marks

            json_dict = fix_JSON(script.string.strip().rstrip(",").replace("\u003c", "<").replace("\u003e", ">").replace("\u0026", "&").replace('&#91;', '[').replace('&#93;', ']').replace("&nbsp", " ")) or {}

            if not json_dict:
                logging.info(kwargs)
                logging.info(script.string)

            return json_dict

    return {}


# Will extract the text from, and concatenate together, all elements of a given selector
def flatten_enclosed_elements(enclosing_element, selector_type, **kwargs):
    if not enclosing_element:
        logging.debug('no enclosing element for flatten_enclosed_elements')
        return None

    selector_type = None if selector_type.lower() == "all" else selector_type
    child_elements = enclosing_element.find_all(selector_type)

    text_list = []
    for ele in child_elements:
        ele_str = extract_stripped_string(ele, **kwargs)
        if isinstance(ele_str, str):
            text_list.append(ele_str)

    join_delim = kwargs.get("delim", ", ")
    return join_delim.join(text_list) if kwargs.get("output_str") or kwargs.get("delim") else text_list


# Will extract the text from selectors nextSibling to the selector you can access. TODO
def flatten_neighboring_selectors(enclosing_element, selector_type, **kwargs):
    if not enclosing_element:
        logging.warning('no enclosing element for flatten_neighboring_selectors')
        return None

    text_list = []
    for ele in enclosing_element.find_all(selector_type):
        next_s = ele.nextSibling
        if not (next_s and isinstance(next_s, NavigableString)):
            continue # TODO extract with .string
        elif next_s and str(next_s):
            text_list.append(extract_stripped_string(next_s, **kwargs))

    return ", ".join(text_list) if kwargs.get("output_str") else text_list


def safely_find_all(parsed, html_type, property_type, identifier, null_value, **kwargs):
    html_tags = parsed.find_all(html_type, {property_type : identifier})

    if not html_tags:
        return null_value

    if kwargs.get("get_link"):
        data = [x.get("href").strip() if x.get("href") else x.a.get("href", "").strip() for x in html_tags]
    else:
        data = [x.get_text(separator=kwargs.get("text_sep", " "), strip=True).replace("\n", "").strip() for x in html_tags]

    if not data:
        return null_value

    data = [x for x in data if x] # drop empty strings
    return ", ".join(data) if kwargs.get("output_str") else data

"""
Supported kwargs
    null_value: any_type - the value you want returned if a null would otherwise be returned
    children: list - an ordered list of child tags you want to walk down into. ex: ["li", "a", "nextSibling"]
    get_link: bool -
# Note: property_type and identifier are effectively case insensitive b/c bs4 parser converts all tag and attribute names to lower case (though otherwise it would be case sensitive) 
"""

def safely_get_text(parsed, html_type, property_type, identifier, **kwargs):
    null_value = kwargs.get("null_value", "")

    try:
        if not parsed:
            return null_value

        if kwargs.pop("find_all", False):
            return safely_find_all(parsed, html_type, property_type, identifier, null_value, **kwargs)

        html_tag = parsed.find(html_type, {property_type : identifier})

        if not html_tag:
            return null_value

        # for nesting into child components. Ex: ["a", "p", "time"]
        for key in kwargs.get("children", []):
            if key == "nextSibling": # necessary, unclear why
                html_tag = html_tag.nextSibling if html_tag else html_tag
            else:
                html_tag = html_tag.key if html_tag else html_tag

        if kwargs.get("get_link") and html_tag:
            if html_tag.get("href"):
                return html_tag.get("href").strip().rstrip("/")
            elif html_tag.a and html_tag.a.get("href"):
                html_tag.a.get("href").strip().rstrip("/") or null_value
        elif html_type == "meta" and html_tag:
            return extract_stripped_string(html_tag.get("content", null_value), null_value=null_value)#.strip().replace("\n", " ")
        else:
            return extract_stripped_string(html_tag, null_value=null_value)

    except Exception as e:
        logging.warning(e)
        return null_value

    return null_value
