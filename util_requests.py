from utility.util import package_response, standardize_event, validate_params, format_url
from utility.util_datastores import scan_dynamodb

import random
import logging
import os
from time import sleep

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


################################# ~ Outbound Requests ~ ####################################

# TODO restore level
def get_ds_proxy_list(**kwargs):
    countries = kwargs.get("countries", "US|CA|MX|AT|BE|HR|CZ|DK|EE|FL|FR|DE|GB|GR|HU|IE|IT|LU|LT|LI|MC|NL|NO|PL|RO|RS|CS|SK|SI|ES|SE|CH|GB")
    url = os.environ["DS_URL"] + f"&showcountry={kwargs.get('show_country', 'no')}&country={countries}&https={kwargs.get('HTTPS', 'yes')}"
    url += "&level=1|2"

    response = api_request(url, "GET", raw_response=True)
    proxies = [x.decode("utf-8") for x in response.iter_lines()] # bc it returns raw text w/ newlines
    logging.info(f"{len(proxies)} proxies were found")
    if kwargs.get('show_country'):
        return [x.split("#") for x in proxies]

    return proxies

def rotate_ds_proxy(proxies):
    if len(proxies) == 0:
        logging.info("Exhausted list; getting another")
        proxies = get_ds_proxy_list()

    proxy = proxies.pop(0)
    return proxy, proxies

def rotate_proxy(proxies):
    if not proxies:
        proxies =  prioritize_proxy(scan_dynamodb('proxyTable'), "US")
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


def handle_request_exception(e, disable_error_messages):
    if "Caused by SSLError(SSLCertVerificationError" in str(e):
        warning = f'-----> ERROR. Request Threw: Certificate Error. {e}<-----'
        message, status_code = None, 495
    elif "Exceeded 30 redirects" in str(e):
        warning = f'-----> ERROR. Request Threw: Too Many Redirects Error. {e}<-----'
        message, status_code = None, 399
    elif "TimeoutError" in str(e):
        warning = f'-----> ERROR. ROTATE YOUR PROXY. Request Threw TimeoutError: {e}<-----'
        message, status_code = f'-----> ERROR. ROTATE YOUR PROXY. Request Threw TimeoutError: {e} <-----', 408
    elif "Caused by NewConnectionError" in str(e) and "ProxyError" not in str(e):
        warning = f'-----> ERROR. EFFECTIVE 404. {e}<-----'
        message, status_code = f'-----> ERROR. ROTATE YOUR PROXY. Request Threw NewConnectionError: {e} <-----', 404
    elif any(x for x in ["HTTPConnectionPool", "MaxRetryError" "ProxyError", "SSLError", "ProtocolError", "ConnectionError", "HTTPError", "Timeout"] if x in str(e)):
        warning = f'-----> ERROR. ROTATE YOUR PROXY. {e}<-----'
        message, status_code = f'-----> ERROR. ROTATE YOUR PROXY. {e} <-----', 601
    else:
        warning = f'-----> ERROR. Request Threw: Unknown Error. {e}<-----'
        message, status_code = f'-----> ERROR. Request Threw: Unknown Error. {e}<-----', 609

    if not disable_error_messages:
        logging.warning(warning)

    return message, status_code


# Mock a browser and visit a site
def site_request(url, proxy, wait, **kwargs):
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
    if not kwargs.get("http_proxy"): headers['upgrade-insecure-requests'] = "1"  # Allow redirects from HTTP -> HTTPS

    try:
        approved_request_kwargs = ["prevent_redirects", "timeout", "hooks"]
        request_kwargs = {k:v for k,v in kwargs.items() if k in approved_request_kwargs}
        request_kwargs["allow_redirects"] = False if request_kwargs.pop("prevent_redirects", None) else True

        if kwargs.get("http_proxy"):
            request_kwargs["proxies"] = {"http": f"http://{proxy}"}
        elif proxy:
            request_kwargs["proxies"] = {"http": f"http://{proxy}", "https": f"https://{proxy}"}

        logging.debug(f"Now requesting {url}")
        response = requests.get(url, headers=headers, **request_kwargs)

    except Exception as e:
        message, applied_status_code = handle_request_exception(e, kwargs.get("disable_error_messages"))
        return message, applied_status_code

    # if response.status_code in [406] and "Mod_Security" in response.text:

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

def extract_stripped_string(html_tag, **kwargs):
    if html_tag and str(html_tag) and isinstance(html_tag, NavigableString):
        return str(html_tag).replace("\n", " ").replace("\r", " ").replace('\\xa0', ' ').strip()

    if not html_tag or not html_tag.get_text():
        return html_tag

    return html_tag.get_text(separator=kwargs.get("text_sep", " "), strip=True).replace("\n", " ").replace("\r", " ").replace('\\xa0', ' ').replace(r"\xa0", " ").replace(u'\xa0', ' ')

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
        data = [x.get("href").strip() if x.get("href") else x.a.get("href", null_value).strip() for x in html_tags]
    else:
        data = [x.get_text(separator=kwargs.get("text_sep", " "), strip=True).replace("\n", "").strip() for x in html_tags]

    if data and kwargs.get("output_str"):
        return ", ".join(data)
    elif data:
        return data
    else:
        return null_value


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
            return html_tag.get("href").strip().rstrip("/") if html_tag.get("href") else html_tag.a.get("href", null_value).strip()
        elif html_type == "meta" and html_tag:
            return html_tag.get("content", null_value)
        elif isinstance(html_tag, NavigableString):
            return str(html_tag).replace("\n", "").replace('\\xa0', ' ').strip() if (html_tag and str(html_tag)) else null_value
        else:
            return html_tag.get_text(separator=kwargs.get("text_sep", " "), strip=True).replace("\n", "").replace('\\xa0', ' ') if (html_tag and html_tag.get_text(separator=kwargs.get("text_sep", " "), strip=True)) else null_value

    except Exception as e:
        logging.warning(e)
        return null_value
