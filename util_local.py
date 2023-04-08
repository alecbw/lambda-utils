from utility.util import invoke_lambda, is_url, format_url
# from utility.util_gspread import open_gsheet # imported below

import json
import csv
import sys
import logging

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
    return data # is list of dicts


def write_to_gsheet(output_lod, sheet, tab, primary_key, **kwargs):

    if sys.getsizeof(output_lod) > (6291456-5000):
        logging.warning("You need to split your data rows")
        return 413 # payload too large

    resp, status_code = invoke_lambda({
            "Gsheet": sheet,
            "Tab": tab,
            "Type": kwargs.get("write_type", "Append_All"),
            "Primary_Key": primary_key,
            "Data": output_lod
        },
        "contextify-serverless-prod-gsheet-write",
        kwargs.get("invoke_type", "RequestResponse"),
    )
    if status_code in [200, 202]:
        print(f"Finished writing to Google Sheet {sheet}. Status code {status_code}")
    else:
        print(f"Error writing to Google Sheet {sheet}. Status code {status_code}; message: {resp}")

    return status_code


# Use if you're hitting the 2MB Lambda limit. This uses the low-level values_append function
def naive_append_gsheet_tab(sheet, tab, output_lod, headers):
    from utility.util_gspread import open_gsheet

    data_lol = []
    for row in output_lod:
        data_lol.append([row.get(x) for x in headers])

    sh, worksheet_list = open_gsheet(sheet)
    resp = sh.values_append(tab +"!A1", {'valueInputOption': 'USER_ENTERED'}, {'values': data_lol})
    print(resp)


########################################################################################################################

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
        # file_lol = [x[kwargs["url_col"]] for x in file_lod if is_url(x[kwargs["url_col"]])] # throw out empty cells
        file_list = [x[kwargs["url_col"]] for x in file_lod if x] # throw out empty cells
        logging.info(f"Length of input CSV after removing falsey rows and accounting for start_at is: {len(file_list)}")
        return file_list

    logging.info(f"Length of input CSV after accounting for start_at is: {len(file_lod)}")
    return file_lod


def write_output_csv(filename, output_lod, **kwargs):
    if not kwargs.get("prevent_csv_suffix") and ".csv" not in filename:
        filename = filename + ".csv"
    if not kwargs.get("prevent_output_prefix") and "Output " not in filename:
        filename = "Output " + filename

    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, kwargs.get("header", output_lod[0].keys()))
        dict_writer.writeheader()
        dict_writer.writerows(output_lod)

    logging.info(f"Write to csv {filename} was successful\n")


def append_to_csv(filename, output_lod, **kwargs):
    filename = filename + ".csv" if ".csv" not in filename else filename

    with open(filename, 'a+', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, kwargs.get("header", output_lod[0].keys()))
        dict_writer.writerows(output_lod)


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


def open_local_json():
    json_files = [f for f in os.listdir('.') if ".json" in f and os.path.isfile(f)]
    if len(json_files) == 0:
        sys.exit("Make sure you have downloaded the .json private key from the API Console GUI")
    elif len(json_files) > 1:
        sys.exit("There's more than one JSON. Remove one or write a function referencing its name.")
    json_file = json_files[0]
    pwd = os.path.dirname(os.path.abspath("__file__"))
    with open(pwd + "/" + json_file) as f_in:
        return(json.load(f_in, strict=False))
