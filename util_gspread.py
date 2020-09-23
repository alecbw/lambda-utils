import os
import json
import time

import gspread
import requests
import jwt # pip install PyJWT
from google.oauth2 import service_account


def auth_gspread():
    auth = {
        "private_key": os.environ["GSHEETS_PRIVATE_KEY"].replace("\\n", "\n").replace('"', ''),
        "client_email": os.environ["GSHEETS_CLIENT_EMAIL"],
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    credentials = service_account.Credentials.from_service_account_info(auth, scopes=scopes)
    return gspread.authorize(credentials)


""" 
Note: the above uses the same overarching Google OAuth2 system. The difference is:
    * auth_gspread uses a service worker JSON
    * auth_google_analytics uses a service worker's 

"""
def gsa_generate_jwt(private_key_json):
    payload = {
        'iss': private_key_json["client_email"], # '123456-compute@developer.gserviceaccount.com',
        'sub': private_key_json["client_email"], #'123456-compute@developer.gserviceaccount.com',
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
            logging.error(e)
            return None, None

    worksheet_list = get_gsheet_worksheet_names(sh)
    return sh, worksheet_list


def get_gsheet_worksheet_names(sh):
    return [x.title for x in sh.worksheets()]


def get_gsheet_tab(sh, tab_name, **kwargs):
    if isinstance(tab_name, int):  # index
        tab = sh.get_worksheet(tab_name)
    elif isinstance(tab_name, str):
        tab = sh.worksheet(tab_name)

    # Weird bug where get_all_records() on an empty sheet will raise IndexError
    try:
      tab_lod = tab.get_all_records(default_blank=kwargs.get("default_blank", None))
    except IndexError:
      tab_lod = []

    return tab, tab_lod


# Create a worksheet.
def create_gsheet_worksheet(sh, tab_name, **kwargs):
    return sh.add_worksheet(
        title=tab_name,
        rows=kwargs.get("rows", "50"),
        cols=kwargs.get("cols", "5")
    )


def simple_tab_append(sheet, tab, data_lol):
    sh, worksheet_list = open_gsheet(sheet)
    sh.values_append(tab, {'valueInputOption': 'USER_ENTERED'}, {'values': data_lol})