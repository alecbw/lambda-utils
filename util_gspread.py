import gspread
from google.oauth2 import service_account

import os


def auth_gspread():
    auth = {
        "private_key": os.environ["GSHEETS_PRIVATE_KEY"].replace("\\n", "\n").replace('"', ''),
        "client_email": os.environ["GSHEETS_CLIENT_EMAIL"],
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = service_account.Credentials.from_service_account_info(auth, scopes=scopes)
    return gspread.authorize(credentials)


def open_gsheet(sheet_name):
    gc = auth_gspread()

    if 'docs.google.com' in sheet_name:
        sh = gc.open_by_url(sheet_name)
    elif len(sheet_name) == 44:
        sh = gc.open_by_key(sheet_name)
    else:  # You must have enabled the Google Drive API in console.developers.google.com to use this
        sh = gc.open(sheet_name)

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
