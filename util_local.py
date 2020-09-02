from utility.util import invoke_lambda, is_url, format_url
from utility.util_gspread import open_gsheet

import json
import csv
import sys


def read_from_gsheet(sheet, tab):

    data, status_code = invoke_lambda({
        "Gsheet": sheet,
        "Tab": tab,
        "Drop_Empty_Rows": True,
    },
    "contextify-serverless-prod-gsheet-read",
    "RequestResponse"
    )
    if status_code in [200, 202]:
        print(f"Finished reading from Google Sheet {sheet}. Status code {status_code}")
    else:
        print(f"Error reading from Google Sheet {sheet}. Status code {status_code}; message: {data}")
    return data


def write_to_gsheet(rows_lod, sheet, tab, primary_key, **kwargs):

    resp, status_code = invoke_lambda({
            "Gsheet": sheet,
            "Tab": tab,
            "Type": kwargs.get("write_type", "Append_All"),
            "Primary_Key": primary_key,
            "Data": rows_lod
        },
        "contextify-serverless-prod-gsheet-write",
        kwargs.get("invoke_type", "RequestResponse"),
    )
    if status_code in [200, 202]:
        print(f"Finished writing to Google Sheet {sheet}. Status code {status_code}")
    else:
        print(f"Error writing to Google Sheet {sheet}. Status code {status_code}; message: {resp}")


def naive_append_gsheet_tab(sheet, tab, data_lod, headers):
    data_lol = []
    for row in data_lod:
        data_lol.append([row.get(x) for x in headers])

    sh, worksheet_list = open_gsheet(sheet)
    resp = sh.values_append(tab, {'valueInputOption': 'USER_ENTERED'}, {'values': data_lol})
    print(resp)
########################################################################################################################


def append_to_csv(rows_lod, csv_type):
    with open(f'output 3 {csv_type}.csv', 'a+', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, rows_lod[0].keys())
        dict_writer.writerows(rows_lod)


def read_input_csv(filename, **kwargs):
    filename = filename + ".csv" if ".csv" not in filename else filename

    with open(filename) as f:
        file_lod = [{k:v for k, v in row.items()} for row in csv.DictReader(f, skipinitialspace=True)] # LOD

    print(f"Length of input CSV is: {len(file_lod)}")

    if kwargs.get("columns") and any(x for x in kwargs["columns"] if x not in file_lod[0].keys()):
        sys.exit(f"Exiting. Your CSV needs to have these columns: {kwargs['columns']}")

    if kwargs.get("start_row"):
        file_lod = file_lod[kwargs["start_row"]:]

    if kwargs.get("url_col"):
        file_lol = [x[kwargs["url_col"]] for x in file_lod if is_url(x[kwargs["url_col"]])] # throw out empty cells
        print(f"Length of input CSV after removing non-URL rows and accounting for start_at is: {len(file_lol)}")
        return file_lol

    print(f"Length of input CSV after accounting for start_at is: {len(file_lod)}")
    return file_lod


def write_output_csv(filename, output_lod):
    filename = filename + ".csv" if ".csv" not in filename else filename

    with open(f"Output {filename}", 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, output_lod[0].keys())
        dict_writer.writeheader()
        dict_writer.writerows(output_lod)

    print(f"Write to csv {'Output ' + filename} was successful\n")


###################################################################################################


# def write_json_to_s3(bucket, folder, url, result_dict, **kwargs):
#     json_str = json.dumps(result_dict)
#     url_filename = format_url(url, remove_trailing_slash=True)
#     if kwargs.get("replace_backslash"):
#         url = url.replace("/", r"\\")
#     write_s3_file(bucket, f"{folder}/{url_filename}.json", json_str)


def write_separate_output_json(filename, output_lod):
    for row in output_lod:
        write_name = "output/Output " + filename + " " + row['URL'].replace("http://", "") + ".json" # if ".json" not in filename else filename
        with open(write_name, 'w') as f:
            json.dump(row, f)

    print("Write to JSON was successful\n")
