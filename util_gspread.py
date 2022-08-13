from utility.util import ez_convert_lod_to_lol

import os
import json
import time

import gspread
import requests
import logging
# import jwt # HAPPENS BELOW
from google.oauth2 import service_account

# MAYBETODO: A more-modern approach uses gspread's service_account_from_dict
# see: https://github.com/burnash/gspread/commit/810183bd5d4d441e9f4d797114d3f49b80939969
def auth_gspread():
    auth = {
        "private_key": os.environ["GSHEETS_PRIVATE_KEY"].replace("\\n", "\n").replace('"', ''),
        "client_email": os.environ["GSHEETS_CLIENT_EMAIL"],
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    credentials = service_account.Credentials.from_service_account_info(auth, scopes=scopes)
    return gspread.authorize(credentials)

def auth_google_analytics():
    auth = {
        "private_key": os.environ["GA_PRIVATE_KEY"].replace("\\n", "\n").replace('"', ''),
        "client_email": os.environ["GA_CLIENT_EMAIL"],
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    scopes = ["https://www.googleapis.com/auth/analytics.readonly"]
    credentials = service_account.Credentials.from_service_account_info(auth, scopes=scopes)
    return gspread.authorize(credentials)


"""
A Google Service Account with configured Oauth2 ClientID can create a long-lived refresh token
That refresh token can then be exchanged for a short-lived (1 hour) access token
We fetch a new access token each invocation because it's easier than storing secrets' state
"""
def service_account_exchange_refresh_token_for_access_token(refresh_token_json):
    refresh_token_json = json.loads(refresh_token_json)
    
    url = "https://accounts.google.com/o/oauth2/token?grant_type=refresh_token"
    url += "&client_secret=" + refresh_token_json["GA_CLIENT_SECRET"]
    url += "&client_id=" + refresh_token_json["GA_CLIENT_ID"]
    url += "&refresh_token=" + refresh_token_json["GA_REFRESH_TOKEN"]

    resp = requests.post(url)
    return resp.json().get("access_token")


""" 
Note: the above uses the same overarching Google OAuth2 system. The difference is:
    * auth_gspread uses a service worker private_key -> client SDK auth'd
    * auth_google_analytics uses a service worker's private_key -> access_key
"""
def gsa_generate_jwt(private_key_json):
    payload = {
        'iss': private_key_json["client_email"],
        'sub': private_key_json["client_email"],
        'iat': time.time(),
        'exp': time.time() + 3600,
        'aud': "https://oauth2.googleapis.com/token", 
        "scope": "https://www.googleapis.com/auth/analytics",
    }
    signed_jwt = jwt.encode(
        payload, 
        private_key_json["private_key"], 
        headers={'kid': private_key_json["private_key_id"]},
        algorithm='RS256'
    )
    return signed_jwt


def gsa_make_jwt_request(signed_jwt):
    headers = {
        'Authorization': f"Bearer {signed_jwt}",
        'content-type': "application/x-www-form-urlencoded",
    }
    body = {
        'grant_type': "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": signed_jwt,
    }

    response = requests.post("https://oauth2.googleapis.com/token", headers=headers, data=body)
    response.raise_for_status()

    return response.json().get("access_token")


def generate_service_account_access_token(SA_PRIVATE_KEY_JSON):
    import jwt # pip install PyJWT

    SA_PRIVATE_KEY_JSON = json.loads(SA_PRIVATE_KEY_JSON)

    jwt = gsa_generate_jwt(SA_PRIVATE_KEY_JSON)

    return gsa_make_jwt_request(jwt)



####################################################################################


def open_gsheet(sheet_name):
    gc = auth_gspread()

    if 'docs.google.com' in sheet_name:
        sh = gc.open_by_url(sheet_name)
    elif len(sheet_name) == 44:
        sh = gc.open_by_key(sheet_name)
    else:  # You must have enabled the Google Drive API in console.developers.google.com to use this
        try:
            sh = gc.open(sheet_name)
        except Exception as e:
            error_message = e if len(str(e)) != 0 else "APIError - Insufficient Permission or sheet doesnt exist"
            logging.error(error_message)
            return None, None

    worksheet_list = get_gsheet_tab_names(sh)
    return sh, worksheet_list


def get_gsheet_tab_names(sh):
    return [x.title for x in sh.worksheets()]


def get_gsheet_tab(sh, tab_name, **kwargs):
    if isinstance(tab_name, int):  # index
        tab = sh.get_worksheet(tab_name) # get_worksheet_by_id TOOD
    elif isinstance(tab_name, str):
        tab = sh.worksheet(tab_name)
    else:
        raise TypeError(f"Unsupported tab_name type in get_gsheet_tab - {tab_name} / {type(tab_name)}")

    # Weird bug where get_all_records() on an empty sheet will raise IndexError
    try:
      tab_lod = tab.get_all_records(default_blank=kwargs.get("default_blank", None))
    except IndexError: # TODO may've been resolved
      tab_lod = []

    return tab, tab_lod


# Create a tab (aka a worksheet).
def create_gsheet_tab(sh, tab_name, **kwargs):
    return sh.add_worksheet(
        title=tab_name,
        rows=kwargs.get("rows", "50"),
        cols=kwargs.get("cols", "5")
    )


"""
perm_type: The account type. Allowed values are: ``user``, ``group``, ``domain``, ``anyone``
role: The primary role for this user. Allowed values are: ``owner``, ``writer``, ``reader``
"""
def create_gsheet_sheet(gc, sheet_name, **kwargs):
    if not gc:
        gc = auth_gspread()

    # if kwargs:
    #     kwargs = {k.lower():v for k,v in kwargs.items()}

    sh = gc.create(sheet_name)
    if kwargs.get("share_with"):
        sh.share(kwargs.get("share_with"), perm_type='user', role=kwargs.get("share_role", "reader"))
    
    return sh


# TODO headers not used if LoL passed
# Note: this will not write empty rows, even if they are present in the data
def simple_tab_append(sh, tab_name, data, headers, **kwargs):
    if isinstance(sh, str):
        sh, worksheet_list = open_gsheet(sh)

    if isinstance(data, list) and isinstance(data[0], dict):
        data = ez_convert_lod_to_lol(data, headers, include_headers=kwargs.get("include_headers"))
    elif not (isinstance(data, list) and isinstance(data[0], list)):
        raise ValueError("You must provide a list of lists or a list of dictionaries to simple_tab_name_append")

    resp = sh.values_append(tab_name, {'valueInputOption': 'USER_ENTERED'}, {'values': data})
    logging.debug(resp)
