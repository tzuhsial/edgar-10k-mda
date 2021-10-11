"""
Functions related to downloading files from Edgar

"""
import concurrent.futures
import csv
import itertools
import os
from glob import glob

import requests
from ratelimit import limits, sleep_and_retry

from .config import REQUESTS_PER_SECOND, HEADERS
from .constants import FORM_INDEX_URL_TEMPLATE
from .util import timeit, write_content


@sleep_and_retry
@limits(calls=REQUESTS_PER_SECOND, period=1)
def download_file(url: str, download_path: str, overwrite: bool = False):
    """
    Downloads file from SEC to disk

    Args:
        url (str)
        download_path (str)
        overwrite (bool): if specified, redownloads

    Returns:
        True if success else False
    """
    if not overwrite and os.path.exists(download_path):
        print("{} already exists. Skipping download...".format(download_path))
        return True
    try:
        print("Requesting {}".format(url))
        res = requests.get(url, headers=HEADERS)
        write_content(res.text, download_path)
        print("Write to {}".format(download_path))
        return True
    except Exception as e:
        print(e)
        return False


def index_url_iterator(start_year: int, end_year: int, quarters: list):
    """
    Iterator that yields url paths for index files

    Yields:
        (index_url, output_name)

    """
    # Prepare argument
    years = range(start_year, end_year + 1)
    for year, qtr in itertools.product(years, quarters):
        output_name = f"{year}.QTR{qtr}.form.idx"
        yield FORM_INDEX_URL_TEMPLATE.format(year, qtr), output_name


def form_url_iterator(index_dir: str, form_type: str):
    """
    Iterator that yields url paths for form files

    Yields:
        (form_url, cik, form_name)

    """
    for index_path in sorted(glob(os.path.join(index_dir, "*.idx"))):
        with open(index_path, "r") as fin:
            arrived = False
            fields_begin = None
            for line in fin.readlines():
                if line.startswith("Form Type"):
                    fields_begin = [
                        line.find("Form Type"),
                        line.find("Company Name"),
                        line.find("CIK"),
                        line.find("Date Filed"),
                        line.find("File Name"),
                    ]
                elif line.startswith(f"{form_type} "):
                    assert fields_begin is not None
                    arrived = True
                    filename = row[-1]
                    form_url = os.path.join(SEC_GOV_URL, filename).replace("\\", "/")
                    cik, output_name = filename.split('/')[-2:]
                    yield form_url, cik, output_name
                elif arrived:  # index files are sorted properly, so we don't need this
                    break