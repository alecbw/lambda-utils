from utility.util import package_response, standardize_event, validate_params, format_url
from utility.util_datastores import scan_dynamodb

import random
import logging
from time import sleep

from bs4 import BeautifulSoup, element, NavigableString
import requests
from urllib3.exceptions import MaxRetryError, ProtocolError
from requests.exceptions import ProxyError, ConnectionError, HTTPError, SSLError, Timeout


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
    encodings = ["gzip, deflate, br, sdch", "gzip, deflate, br"]
    return random.choice(encodings)


def rotate_language():
    languages = ["en-US,en;q=0.8", "en-US,en;q=0.9"]
    return random.choice(languages)


def rotate_accept():
    accepts = ["text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9"]
    return random.choice(accepts)


################################# ~ Outbound Requests ~ ####################################


# Sorts the list of proxies by location so the specified locations' proxies are first
def prioritize_proxy(proxies, location):
    output_proxies_list = []
    for proxy in proxies:
        if proxy.get("location") == location:
            output_proxies_list.insert(0, proxy)
        else:
            output_proxies_list.append(proxy)
    return output_proxies_list


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
        'upgrade-insecure-requests': "1",                        # Allow redirects from HTTP -> HTTPS
        'DNT': "1",                                              # Ask the server to not be tracked (lol)
    }
    try:
        request_kwargs = {}
        if proxy:
            request_kwargs["proxies"] = {"http": f"http://{proxy}", "https": f"https://{proxy}"}

        # TODO needs more testing
        if kwargs.get("prevent_redirects"):
            request_kwargs["allow_redirects"] = False

        response = requests.get(url, headers=headers, **request_kwargs)

    except (MaxRetryError, ProxyError, SSLError, ProtocolError, Timeout, ConnectionError, HTTPError) as e:
        logging.warning(f'-----> ERROR. ROTATE YOUR PROXY. {e}<-----')
        return '-----> ERROR. ROTATE YOUR PROXY. <-----', 666
    except Exception as e:
        logging.warning(f'-----> ERROR. Request Threw: Unknown Error. {e}<-----')
        return '-----> ERROR. Request Threw: Unknown Error. <-----', 666

    if response.status_code not in [200, 202, 301, 302]:
        logging.warning(f'-----> ERROR. Request Threw: {response.status_code} <-----')
    if response.status_code in [502, 503, 999]:
        return f'-----> ERROR. Request Threw: {response.status_code}. ROTATE YOUR PROXY <-----', 666

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


# Will extract the text from, and concatenate together, all elements of a given selector
def flatten_enclosed_elements(enclosing_element, selector_type, **kwargs):
    if not enclosing_element:
        logging.warning('no enclosing element for flatten_enclosed_elements')
        return None

    text_list = []
    for ele in enclosing_element.findAll(selector_type):
        if ele and ele.get_text():
            text_list.append(ele.get_text().strip().replace("\n", "").replace("\r", ""))

    return ", ".join(text_list) if kwargs.get("output_str") else text_list


# Will extract the text from selectors nextSibling to the selector you can access. TODO
def flatten_neighboring_selectors(enclosing_element, selector_type, **kwargs):
    if not enclosing_element:
        logging.warning('no enclosing element for flatten_neighboring_selectors')
        return None

    text_list = []
    for ele in enclosing_element.findAll(selector_type):
        next_s = ele.nextSibling
        if not (next_s and isinstance(next_s, NavigableString)):
            continue # TODO extract with .string
        elif next_s and str(next_s):
            text_list.append(next_s.get_text().strip().replace("\n", "").replace("\r", ""))
    return ", ".join(text_list) if kwargs.get("output_str") else text_list


def safely_find_all(parsed, html_type, property_type, identifier, null_value, **kwargs):
    html_tags = parsed.find_all(html_type, {property_type : identifier})
    data = [x.get_text().replace("\n", "").strip() for x in html_tags] if html_tags else None

    if data and kwargs.pop("output_str", False):
        return ", ".join(data)
    elif data:
        return data
    else:
        return null_value

def safely_get_text(parsed, html_type, property_type, identifier, **kwargs):
    null_value = kwargs.get("null_value", "")
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

    if isinstance(html_tag, NavigableString):
        return str(html_tag).replace("\n", "").strip() if (html_tag and str(html_tag)) else null_value
    else:
        return html_tag.get_text().replace("\n", "").strip() if (html_tag and html_tag.get_text().strip()) else null_value
