"""
scripts/get-latest-ecs-csv.py
Script to download the latest version of the ECS schema CSV containing all fields,
rotate (if exists) the existing file at Elastic/data/ecs.csv to a versioned file,
and update Elastic/data/ecs.csv with the latest data, and moves the older schema
to a file such as Elastic/data/ecs.SCHEMA_VERSION.csv
ex: Elastic/data/ecs.9.3.0-dev.csv
"""
import csv
import sys
from pathlib import Path
from typing import Any, Dict, List, OrderedDict
from urllib.request import urlopen
import os

BASE_DIR = Path(__file__).parent.parent
ECS_CSV_URL = "https://raw.githubusercontent.com/elastic/ecs/refs/heads/main/generated/csv/fields.csv"
ECS_DIR = BASE_DIR / "data" / "Elastic"
ECS_CSV_FILE = ECS_DIR / "ecs.csv"
ECS_CSV_TEMFILE = ECS_DIR / "ecs-tempfile.csv"
ECS_CSV_COLUMN_COUNT = 9

CACHE = {"CONTENT": {"OLD":None, "NEW":None, "CHANGES":None, "RAW": None}}

def _read_data(file: Path) -> list:
    """Read in the raw data from the local file containing a CSV dump of the given
    Elastic schema (https://github.com/elastic/ecs/blob/main/generated/csv/fields.csv)
    """
    with open(file, mode='r') as f:
        reader = csv.reader(f)
        data = [l for l in reader]
        f.close()
    return data

def _has_data(rawfile: list) -> bool:
    """Check if the CSV actually has any data that we're looking for
    """
    try:
        assert len(rawfile) > 1, "No data found in %s, Line Count: %s" % (str(ECS_CSV_FILE), str(len(rawfile)))
        return True
    except AssertionError as e:
        print(e)
    return False

def _has_right_column_number(rawfile: list) -> bool:
    """Make sure the right number of columns exist and we're
    not shooting our foot off
    """
    try:
        assert len(rawfile[0]) == ECS_CSV_COLUMN_COUNT, "Wrong header length in %s, Column Count: %s, expected %s" % (str(rawfile), str(len(rawfile[0])), str(ECS_CSV_COLUMN_COUNT))
        return True
    except AssertionError as e:
        print(e)
    return False

def _format_data(rawfile: list) -> list:
    """Format the ingested CSV into a list of dicts
    """
    return [dict(zip(rawfile[0], l)) for l in rawfile[1:]]

def _clean_data(data: list) -> list:
    """Clean up the csv, convert '' values to None,
    and text booleans to python object bools
    """
    for d in data:
        d['Indexed'] = bool(d['Indexed'])
        if d["Normalization"] == '':
            d["Normalization"] = None
        if d["Example"] == '':
            d["Example"] = None
    return data

def readfile(file:Path) -> List[Dict[str, Any]]:
    """Read in the csv and clean it up for processing
    """
    data = _read_data(file)
    if not _has_data(data):
        return []
    if not _has_right_column_number(data):
        return []
    data = _format_data(data)
    data = _clean_data(data)
    return data

def _download_data(url: str) -> str:
    with urlopen(url) as webpage:
        content = webpage.read().decode()
    CACHE["CONTENT"]["RAW"] = content
    return content

def _save_downloaded_content(content: str) -> None:
    with open(ECS_CSV_TEMFILE, "w") as destfile:
        destfile.write(content)
        destfile.close()

def download(url: str) -> None:
    new_content = _download_data(url)
    _save_downloaded_content(new_content)

def _load_cache_old() -> None:
    CACHE["CONTENT"]["OLD"] = readfile(ECS_CSV_FILE)

def _load_cache_new(url: str) -> None:
    download(url)
    CACHE["CONTENT"]["NEW"] = readfile(ECS_CSV_TEMFILE)

def _check_new_cache_loaded() -> None:
    if CACHE["CONTENT"]["NEW"] is None:
        raise Exception("Unable to find new content?")

def _init_changes_cache() -> None:
    if CACHE["CONTENT"]["CHANGES"] is None or not isinstance(CACHE["CONTENT"]["CHANGES"], list):
        CACHE["CONTENT"]["CHANGES"] = []

def init_cache(url: str) -> None:
    _load_cache_old()
    _load_cache_new(url)
    _check_new_cache_loaded() 
    _init_changes_cache()

def _ECS_Version_changed() -> bool:
    has_changes = False
    old_schema_items = set([x["ECS_Version"] for x in CACHE["CONTENT"]["OLD"]])
    new_schema_items = set([x["ECS_Version"] for x in CACHE["CONTENT"]["NEW"]])
    if len(old_schema_items) != len(new_schema_items):
        return True
    for i, v in enumerate(old_schema_items):
        for ii, vv in enumerate(new_schema_items):
            if v != vv:
                has_changes = True
            if has_changes:
                break
        if has_changes:
            break
    return has_changes

def _length_changed() -> bool:
    has_changes = False
    if CACHE["CONTENT"]["NEW"] is None:
        return False
    if CACHE["CONTENT"]["OLD"] is None:
        return True
    if len(CACHE["CONTENT"]["OLD"]) != len(CACHE["CONTENT"]["NEW"]):
        has_changes = True
    return has_changes

def _lines_changed() -> bool:
    has_changes = False
    old_hashmap = {hash(str(v)): v for v in CACHE["CONTENT"]["OLD"]}
    new_hashmap = {hash(str(v)): v for v in CACHE["CONTENT"]["NEW"]}
    for k, v in new_hashmap.items():
        if k not in old_hashmap.keys():
            has_changes = True
        if has_changes:
            break
    return has_changes

def content_has_changed() -> bool:
    has_changes = False
    if _length_changed():
        return True
    if _ECS_Version_changed():
        return True
    if _lines_changed():
        return True

    return has_changes



def update_content() -> None:
    oldfile_schema = CACHE["CONTENT"]["OLD"][-1]["ECS_Version"]
    oldfile_name = ECS_CSV_FILE.name.replace(".csv",f".{oldfile_schema}.csv")
    oldfile = ECS_CSV_FILE.parent / oldfile_name
    print(f"Moving old schema ({oldfile_schema}) to {oldfile_name.name}")
    with open(ECS_CSV_FILE, "r") as oc:
        oldcontent = oc.read()
        oc.close()
    with open(oldfile, "w") as of:
        of.write(oldcontent)
        of.close()
    with open(ECS_CSV_FILE, "w") as nf:
        nf.write(CACHE["CONTENT"]["RAW"])
        nf.close()
    print("Finished updating content")

def cleanup(verbose: bool = True) -> None:
    if ECS_CSV_TEMFILE.exists():
        if verbose:
            print(f"Removing tempfile: {ECS_CSV_TEMFILE}")
        os.remove(ECS_CSV_TEMFILE)
    if verbose:
        print("Cleanup complete")

def main(url: str) -> None:
    cleanup(False)
    init_cache(url)
    try:
        has_changes = content_has_changed()
    except Exception as e:
        print(str(e))
        sys.exit(1)
    if has_changes:
        print("Content changes detected, saving new schema...")
        update_content()
    cleanup(True)
    sys.exit(0)



if __name__ == "__main__":
    main(ECS_CSV_URL)
