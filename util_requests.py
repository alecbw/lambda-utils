from utility.util import package_response, standardize_event, validate_params, format_url, fix_JSON, replace_string_char_by_index, startswith_replace, endswith_replace, ez_re_find
from utility.util_datastores import scan_dynamodb

import random
import logging
import os
from time import sleep
import warnings
import json
import re
from urllib.parse import urlencode
from html import unescape
from datetime import datetime, timedelta

from bs4 import BeautifulSoup, element, NavigableString, Tag
import requests


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
                "www.google.com", "www.google.com", "www.google.com", "www.google.com", "www.google.com", "www.google.com"
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
    url = os.environ["DS_URL"] + f"&showcountry={kwargs.get('show_country', 'no')}&https={kwargs.get('HTTPS', 'yes')}"
    url += f"&country={countries}" #
    url += "&level=1|2"
    # print(url)

    response = api_request(url, "GET", raw_response=True)
    proxies = [x.decode("utf-8") for x in response.iter_lines()] # bc it returns raw text w/ newlines
    logging.info(f"{len(proxies)} proxies were found (DS) - {kwargs}")
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

def cache_proxy_list(**kwargs):
    if not os.getenv("_LAST_FETCHED_PROXIES") or ( datetime.strptime(os.environ["_LAST_FETCHED_PROXIES"], '%Y-%m-%d %H:%M:%S') < datetime.utcnow() - timedelta(minutes=8) ):
        proxy_list = scan_dynamodb('proxyTable', output="datetime_str")
        if kwargs.get("shuffle_list"):
            random.shuffle(proxy_list)
        else:
            proxy_list = prioritize_proxy(proxy_list, "US")

        os.environ["_PROXY_LIST"] = json.dumps(proxy_list)
        os.environ["_LAST_FETCHED_PROXIES"] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        return proxy_list
    else:
        print('loading from cache')
        return json.loads(os.environ["_PROXY_LIST"])


def rotate_proxy(proxies, **kwargs):
    if not proxies or kwargs.get("force_scan"):
        proxies = cache_proxy_list(**kwargs) # prioritize_proxy(scan_dynamodb('proxyTable'), "US")

    if kwargs.get("return_proxy_dict"):
        return proxies.pop(0), proxies

    if kwargs.get("HTTPS") in ["True", "true", True]:
        https_proxy = next((x for x in proxies if x.get("HTTPS") == "Y"))
        return proxies.pop(proxies.index(https_proxy)).get("full"), proxies

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

# Your proxy appears to only use HTTP and not HTTPS, try changing your proxy URL to be HTTP

def handle_request_exception(e, proxy, url, disable_error_messages):
    if any(x for x in ["Caused by SSLError(SSLCertVerificationError", "SSL: WRONG_VERSION_NUMBER", "[Errno 65] No route to host", "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: certificate has expired", 'Caused by SSLError(CertificateError("hostname'] if x in str(e)):  # CertificateError -> downgrade to HTTP
        warning = f'-----> ERROR. URL: {url}. Proxy: {proxy}. Request Threw: Certificate Error. {e}<-----'
        status_code = 495
    elif "Exceeded 30 redirects" in str(e):
        warning = f'-----> ERROR. URL: {url}. Proxy: {proxy}. Request Threw: Too Many Redirects Error. {e}<-----'
        status_code = 399
    elif "TimeoutError" in str(e) or " Read timed out." in str(e) or "timeout('timed out')" in str(e):
        warning = f'-----> ERROR. URL: {url}. ROTATE YOUR PROXY. Proxy: {proxy}. Request Threw TimeoutError: {e} <-----'
        status_code = 408
    elif "Caused by NewConnectionError" in str(e) and "ProxyError" not in str(e):
        warning = f'-----> ERROR. URL: {url}. ROTATE YOUR PROXY. Proxy: {proxy}. Effective 404 - Request Threw NewConnectionError: {e} <-----'
        status_code = 404
    elif "Tunnel connection failed: 404 Not Found" in str(e):
        warning = f'-----> ERROR. URL: {url}. ROTATE YOUR PROXY. Proxy: {proxy}. Effective 404 - Request Threw OSError: {e} <-----'
        status_code = 404
    elif "Tunnel connection failed: 503 Service Unavailable" in str(e): # this MAY be a proxy problem and it may be a true 503 from the domain. Only happens with a proxy.
        warning = f'-----> ERROR. Url: {url}. ROTATE YOUR PROXY. Proxy: {proxy}. Request Threw: OSError Error. {e}<-----'
        status_code = 503
    elif "Tunnel connection failed: 403 Forbidden" in str(e): # this MAY be a proxy problem and it may be a true 403 from the domain. Only happens with a proxy.
        warning = f'-----> ERROR. Url: {url}. ROTATE YOUR PROXY. Proxy: {proxy}. Request Threw: OSError Error. {e}<-----'
        status_code = 403
    elif "Connection refused" in str(e) or "Connection reset by peer" in str(e): # or "Remote end closed connection" in str(e):
        warning = f'-----> ERROR. URL: {url}. ROTATE YOUR PROXY. Proxy: {proxy}. Proxy refusing traffic {e} <-----'
        status_code = 602
    elif any(x for x in ["HTTPConnectionPool", "MaxRetryError" "ProxyError", "SSLError", "ProtocolError", "ConnectionError", "HTTPError", "Timeout"] if x in str(e)):
        warning = f'-----> ERROR. URL: {url}. ROTATE YOUR PROXY. Proxy: {proxy}. {e}<-----'
        status_code = 601
    elif any(x for x in ["UnicodeError"] if x in str(e)):
        warning = f'-----> ERROR. Url: {url}. Proxy: {proxy}. Request Threw: UnicodeError Error. {e}<-----'
        status_code = 609
    else:
        warning = f'-----> ERROR. Url: {url}. Proxy: {proxy}. Request Threw: Unknown Error. {e}<-----'
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

        if kwargs.get("http_proxy"): # if you request a https site anyways, the proxy WILL NOT be used
            request_kwargs["proxies"] = {"http": f"http://{proxy}"}
        elif isinstance(proxy, dict): # preformatted
            request_kwargs["proxies"] = proxy
        elif proxy:
            request_kwargs["proxies"] = {"http": f"http://{proxy}", "https": f"https://{proxy}"}

        logging.debug(f"Now requesting {url}")
        response = requests.get(url, headers=headers, **request_kwargs)

    except Exception as e:
        message, applied_status_code = handle_request_exception(e, proxy, url, kwargs.get("disable_error_messages"))
        return message, applied_status_code

    if response.status_code in [502, 503, 999] and not kwargs.get("disable_error_messages"):
        logging.warning(f'-----> ERROR. Url: {url}. Request Threw: {response.status_code}. ROTATE YOUR PROXY <-----')
    elif response.status_code not in [200, 202, 301, 302] and not kwargs.get("disable_error_messages"):
        logging.warning(f'-----> ERROR. Url: {url}. Request Threw: {response.status_code} <-----')

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
    elif not input_str:
        return input_str

    if kwargs.get("reduce_interior_whitespace"): # internal whitespace can be regexed out, but it can be slow
        input_str = re.sub(r"\s{2,}", " ", input_str)

    # if r'\u' in input_str: # there's unicode characters in an otherwise UTF string
    #     logging.debug(f"there's unicode characters in an otherwise UTF string in ez_strip_str - {input_str}")
    #     input_str = input_str.encode().decode('unicode-escape')


    return input_str.replace(" \n", "").replace(" \r", "").replace("\n ", "").replace("\r ", "").replace("\n", " ").replace(r"\\n", " ").replace("\r", " ").replace('\\xa0', ' ').replace(r"\xa0", " ").replace(r"\u0027", "'").replace(u'\xa0', ' ').replace("&nbsp", " ").replace("•", " ").replace("%20", " ").replace(r"\ufeff", " ").replace("&amp;", "&").replace("&#038;", "&").replace(r"\u0026", "&").replace("&#039;", "'").replace("&#39;", "'").replace("&#8217;", "'").replace("u0022", '"').replace("&quot;", '"').replace("&#8211;", "-").replace("&ndash;", "-").replace(r"\u003c", "<").replace("&lt;", "<").replace(r"\u003e", ">").replace("&gt;", ">").replace('&#91;', '[').replace('&#93;', ']').replace('&#64;', '@').replace("&#46;", ".").replace('%26', '&').strip()


# TODO replace dumbass implementation of replacing newline chars
def extract_stripped_string(html_tag_or_str, **kwargs):
    if not html_tag_or_str:
        return kwargs.get("null_value", html_tag_or_str)

    elif isinstance(html_tag_or_str, NavigableString) and str(html_tag_or_str):
        return ez_strip_str(str(html_tag_or_str))#.replace(" \n", "").replace(" \r", "").replace("\n ", "").replace("\r ", "").replace("\n", " ").replace("\r", " ").replace('\\xa0', ' ').replace(r"\xa0", " ").replace(u'\xa0', ' ').strip()

    elif isinstance(html_tag_or_str, str):
        return ez_strip_str(html_tag_or_str)

    elif isinstance(html_tag_or_str, Tag):
        return ez_strip_str(html_tag_or_str.get_text(separator=kwargs.get("text_sep", " "), strip=True))#.replace(" \n", "").replace(" \r", "").replace("\n ", "").replace("\r ", "").replace("\n", " ").replace("\r", " ").replace('\\xa0', ' ').replace(r"\xa0", " ").replace(u'\xa0', ' ')

    return kwargs.get("null_value", html_tag_or_str)


# [ ] deal with special apostrophe ’ ?
# [ ] need to figure out what to do with encode().decode() logic and resulting
def get_script_json_by_contained_phrase(parsed, phrase_str, **kwargs):
    if not parsed:
        return {} if not kwargs.get("return_string") else ""

    find_all_kwargs = {k:v for k,v in kwargs.items() if k in ["id", "href", "attrs", "type", "name", "property"]}
    for script in parsed.find_all('script', **find_all_kwargs):
        if script and script.string and phrase_str in script.string:
            script_string = script.string.strip()
            if kwargs.get("lstrip"):
                script_string = script_string.lstrip(kwargs['lstrip'])

            if kwargs.get("html_unescape"):
                if kwargs.get("always_escape_quote"):
                    script_string = script_string.replace('&quot;', r'\"')
                script_string = unescape(script_string)
                if r'\u' in script_string: # there's unicode characters in an otherwise UTF string
                    logging.debug("there's unicode characters in an otherwise UTF string")
                    # logging.debug(script_string)
                    # logging.debug(script_string.encode().decode('unicode-escape').encode('latin-1').decode('utf-8'))
                    # script_string = script_string.encode().decode('unicode-escape')

            if kwargs.get("return_string"):
                return script_string.strip().rstrip(",")


            while '“' in script_string or '”' in script_string or "&quot;" in script_string: # TODO maybe this logic should be in fix_JSON
                char_index = next((script_string.find(x) for x in ['“', '”', '&quot;'] if script_string.find(x) != -1), None)
                if not char_index:
                    break
                elif (not kwargs.get("always_escape_quote") and (":" in script_string[char_index-2:char_index+3] or "," in script_string[char_index-2:char_index+3])): # maybe the always_escape_quote logic should be separate of the above always_escape_quote logic. MAYBETODO
                    script_string = replace_string_char_by_index(script_string, char_index, '"') # leading or trailing quote of key or value
                else:
                    script_string = replace_string_char_by_index(script_string, char_index, r'\"') # internal quotation mark, must be escaped

            script_string = startswith_replace(script_string, ["// <![CDATA[", "//<![CDATA[", "/*<![CDATA[*/", "/* <![CDATA[  */", "execOnReady(function(){", "setTimeout(function(){"], "") # some sites include comments that break json.load, so we remove them before trying to load
            script_string = endswith_replace(script_string, ["// ]]>", "//]]>", "/*]]>*/", "/*  ]]> */", "});", "},3000);"], "")

            json_dict = fix_JSON(ez_strip_str(script_string.rstrip(",").rstrip(";")), recursion_limit=200, log_on_error=kwargs.get('url')) or {}

            if json_dict:
                return json_dict
            else: # continue; there may be >1 ld+json onsite, and one of the others may work
                logging.info(kwargs)
                logging.debug(script_string)


    return {} if not kwargs.get("return_string") else ""



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

    if property_type == 'string':
        html_tags = parsed.find_all(html_type, string=identifier)
    else:
        html_tags = parsed.find_all(html_type, {property_type : identifier})

    if not html_tags:
        return null_value

    # TODO - children support?

    if kwargs.get("get_link"):
        data = [x.get("href").strip() if x.get("href") else (x.a.get("href", "").strip() if x.a else "") for x in html_tags]
    elif kwargs.get("get_src"):
        data = [x.get("src").strip() if x.get("src") else null_value for x in html_tags]
    elif kwargs.get("get_title"):
        data = [x.get("title").strip() if x.get("title") else null_value for x in html_tags]
    elif kwargs.get("get_alt"):
        data = [x.get("alt").strip() if x.get("alt") else null_value for x in html_tags]
    elif kwargs.get("get_value"):
        data = [x.get("value").strip() if x.get("value") else null_value for x in html_tags]
    elif kwargs.get("get_onclick"):
        data = [x.get("onclick").strip() if x.get("onclick") else null_value for x in html_tags]
    elif kwargs.get("get_background_image_url"):
        data = [ez_re_find('(background-image\: url\(\"?)(.*?)(\"?\))', x.get('style'), group=1) if x.get('style') else null_value for x in html_tags]
    elif html_type == "meta" and html_tags:
        data = [extract_stripped_string(x.get("content", null_value), null_value=null_value) for x in html_tags]
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

        if property_type == 'string':
            html_tag = parsed.find(html_type, string=identifier)
        elif not html_type and not property_type: # just want to get text of passed in element, not to drill down
            html_tag = parsed
        else:
            html_tag = parsed.find(html_type, {property_type : identifier})

        if not html_tag:
            return null_value

        # for nesting into child components. Ex: ["a", "p", "time"]
        for key in kwargs.get("children", []):
            html_tag = getattr(html_tag, key) if getattr(html_tag, key) else html_tag


        if kwargs.get("get_link") and html_tag:
            if html_tag.get("href"):
                return html_tag.get("href").strip().rstrip("/")
            elif html_tag.a and html_tag.a.get("href"):
                return html_tag.a.get("href").strip().rstrip("/") or null_value
        elif kwargs.get("get_src"):
            return html_tag.get("src").strip() if html_tag.get("src") else null_value
        elif kwargs.get("get_title"):
            return html_tag.get("title").strip() if html_tag.get("title") else null_value
        elif kwargs.get("get_alt"):
            return html_tag.get("alt").strip() if html_tag.get("alt") else null_value
        elif kwargs.get("get_value"):
            return html_tag.get("value").strip() if html_tag.get("value") else null_value
        elif kwargs.get("get_onclick"):
            return html_tag.get("onclick").strip() if html_tag.get("onclick") else null_value
        elif kwargs.get("get_background_image_url"):
            return ez_re_find('(background-image\: url\(\"?)(.*?)(\"?\))', html_tag.get('style'), group=1) if html_tag.get('style') else null_value
        elif html_type == "meta" and html_tag:
            return extract_stripped_string(html_tag.get("content", null_value), null_value=null_value)#.strip().replace("\n", " ")
        else:
            return extract_stripped_string(html_tag, null_value=null_value)

    except Exception as e:
        if not kwargs.get('disable_print'):
            logging.warning(f"Exception found in safely_get_text: {e}")
        return null_value

    return null_value


def safely_encode_text(parsed, **kwargs):
    if not parsed:
        return "", None

    truncate_at = kwargs.get('truncate_at', 1_000_000)
    try:
        if isinstance(parsed, str):
            text = parsed
        else:
            text = parsed.get_text(separator=" ", strip=True)                           # extract_full_site_text(parsed, drop_duplicates=True)
        _ = text.encode('utf-8') # to trigger error - eg "UnicodeEncodeError: 'utf-8' codec can't encode characters in position 2435-2436: surrogates not allowed"

        text = text[:truncate_at].replace("<br>", " ") # truncate to 1,000,000 characters to avoid Size of a 'single row or its columns cannot exceed 32 MB' Athena error
        encoding = 'utf-8'
    except UnicodeEncodeError as e:
        if isinstance(parsed, str):
            text = parsed.encode('utf-8', errors='replace').decode('utf-8')
        else:
            text = parsed.get_text(separator=" ", strip=True).encode('utf-8', errors='replace').decode('utf-8') # into bytes and back to str
        encoding = f"BROKE_UTF8 {kwargs.get('encoding', '')}".strip()
        logging.warning(e)
        logging.warning(f"The site {kwargs.get('url')} broke text encoding. Provided encoding: {kwargs.get('encoding')}")

    return text, encoding


def add_querystrings_to_a_tags(html, dict_to_add):
    parsed = BeautifulSoup(html, 'html.parser')

    for a_tag in parsed.find_all('a'):
        if a_tag.get('href') and "mailto:" in a_tag['href']:
            continue
        if a_tag.get('href') and "?" in a_tag['href']:
            a_tag['href'] = a_tag['href'] + urlencode(dict_to_add)
        elif a_tag.get('href'):
            a_tag['href'] = a_tag['href'] + "?" + urlencode(dict_to_add)

    return parsed


def modify_html_template(html_template, to_replace_lot, **kwargs):
    for to_replace_tuple in to_replace_lot:
        html_template = html_template.replace(to_replace_tuple[0], to_replace_tuple[1])

    if kwargs.get("utm_dict"):
        html_template = str(add_querystrings_to_a_tags(html_template, kwargs['utm_dict']))

    return html_template

