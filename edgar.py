"""
A standalone script to download and parse edgar 10k MDA section
"""

import argparse
import csv
import concurrent.futures
import itertools
import os
import time
import re
import unicodedata
from functools import wraps
from glob import glob

import requests
from ratelimit import limits, sleep_and_retry
from bs4 import BeautifulSoup

headers = {
    "User-Agent": "YOUR EMAIL HERE",
    "Accept-Encoding": "gzip",
    "Host": "www.sec.gov",
}

SEC_GOV_URL = "https://www.sec.gov/Archives"
FORM_INDEX_URL = os.path.join(
    SEC_GOV_URL, "edgar", "full-index", "{}", "QTR{}", "form.idx"
)

# Used to combine form 10k index files. Adds URL column for lookup
INDEX_HEADERS = ["Form Type", "Company Name", "CIK", "Date Filed", "File Name", "Url"]


def create_parser():
    """Argument Parser"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s", "--start_year", type=int, required=True, help="year to start"
    )
    parser.add_argument("-e", "--end_year", type=int, required=True, help="year to end")
    parser.add_argument(
        "-q",
        "--quarters",
        type=int,
        nargs="+",
        default=[1, 2, 3, 4],
        help="quarters to download for start to end years",
    )
    parser.add_argument(
        "-d", "--data_dir", type=str, default="./data", help="path to save data"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="If True, overwrites downloads and processed files.",
    )
    parser.add_argument("--debug", action="store_true", help="Debug mode")
    return parser


def main():
    # Parse arguments
    parser = create_parser()
    args = parser.parse_args()

    # Download indices
    index_dir = os.path.join(args.data_dir, "index")
    download_indices(
        args.start_year, args.end_year, args.quarters, index_dir, args.overwrite
    )

    # Combine indices to csv
    combine_indices_to_csv(index_dir)

    # Download forms
    form_dir = os.path.join(args.data_dir, "form10k")
    download_forms(index_dir, form_dir, args.overwrite, args.debug)

    # Normalize forms
    parsed_form_dir = os.path.join(args.data_dir, "form10k.parsed")
    parse_html_multiprocess(form_dir, parsed_form_dir, args.overwrite)

    # Parse MDA
    mda_dir = os.path.join(args.data_dir, "mda")
    parse_mda_multiprocess(parsed_form_dir, mda_dir, args.overwrite)


CALLS = 10
RATE_LIMIT = 1


@sleep_and_retry
@limits(calls=CALLS, period=RATE_LIMIT)
def download_file(url: str, download_path: str, overwrite: bool = False):
    """Downloads file to disk
    Args:
        url (str)
        download_path (str)
    Returns:
        True if success else False
    """
    if not overwrite and os.path.exists(download_path):
        print("{} already exists. Skipping download...".format(download_path))
        return True
    try:
        print("Requesting {}".format(url))
        res = requests.get(url, headers=headers)
        write_content(res.text, download_path)
        print("Write to {}".format(download_path))
        return True
    except Exception as e:
        print(e)
        return False


def write_content(content, output_path):
    """Writes content to file
    Args:
        content (str)
        output_path (str): path to output file
    """
    with open(output_path, "w", encoding="utf-8") as fout:
        fout.write(content)


def timeit(f):
    @wraps(f)
    def wrapper(*args, **kw):
        start_time = time.time()
        result = f(*args, **kw)
        end_time = time.time()
        print("{} took {:.2f} seconds.".format(f.__name__, end_time - start_time))
        return result

    return wrapper


@timeit
def download_indices(
    start_year: int, end_year: int, quarters: list, index_dir: str, overwrite: bool
):
    """Downloads edgar 10k form indices with multiprocess
    Args:
        start_year (int): starting year
        end_year (int): ending year
    """
    # Create output directory
    os.makedirs(index_dir, exist_ok=True)

    # Prepare arguments
    years = range(start_year, end_year + 1)
    urls = [
        FORM_INDEX_URL.format(year, qtr)
        for year, qtr in itertools.product(years, quarters)
    ]
    download_paths = [
        os.path.join(index_dir, "year{}.qtr{}.idx".format(year, qtr))
        for year, qtr in itertools.product(years, quarters)
    ]

    # Download indices
    with concurrent.futures.ThreadPoolExecutor(10) as executor:
        for url, download_path in zip(urls, download_paths):
            executor.submit(download_file, url, download_path)


def parse_line_to_record(line, fields_begin):
    """
    Example:
    10-K        1347 Capital Corp                                             1606163     2016-03-21  edgar/data/1606163/0001144204-16-089184.txt

    Returns:
    ["10-K", "1347 Capital Corp","160613", "2016-03-21", "edgar/data/1606163/0001144204-16-089184.txt"]
    """
    record = []
    fields_indices = fields_begin + [len(line)]
    for begin, end in zip(fields_indices[:-1], fields_indices[1:]):
        field = line[begin:end].rstrip()
        field = field.strip('"')
        record.append(field)
    return record


@timeit
def combine_indices_to_csv(index_dir):
    """Combines index files in index_dir csv file for lookup
    Args:
        index_dir (str)
    """
    # Reads all rows into memory
    rows = []
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
                    print(fields_begin)
                elif line.startswith("10-K "):
                    assert fields_begin is not None
                    arrived = True
                    row = parse_line_to_record(line, fields_begin)
                    filename = row[-1]
                    url = os.path.join(SEC_GOV_URL, filename).replace("\\", "/")
                    row = row + [url]
                    rows.append(row)
                elif arrived:
                    break

    # Write to output file
    csv_file = os.path.join(index_dir, "combined.csv")
    with open(csv_file, "w") as fout:
        writer = csv.writer(fout, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(INDEX_HEADERS)
        writer.writerows(rows)


@timeit
def download_forms(
    index_dir: str, form_dir: str, overwrite: bool = False, debug: bool = False
):
    """Reads indices and download forms
    Args:
        index_dir (str)
        form_dir (str)
    """
    # Create output directory
    os.makedirs(form_dir, exist_ok=True)

    # Prepare arguments
    combined_csv = os.path.join(index_dir, "combined.csv")
    print("Combining index files to {}".format(combined_csv))
    urls = read_url_from_combined_csv(combined_csv)

    download_paths = []
    for url in urls:
        download_name = "_".join(url.split("/")[-2:])
        download_path = os.path.join(form_dir, download_name)
        download_paths.append(download_path)

    # Debug
    if debug:
        print("Debug: download only 10 forms")
        download_paths = download_paths[:10]

    # Download forms
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for url, download_path in zip(urls, download_paths):
            executor.submit(download_file, url, download_path, overwrite)


def read_url_from_combined_csv(csv_path):
    """Reads url from csv file
    Args:
        csv_path (str): path to index file
    Returns
        urls: urls in combined csv
    """
    urls = []
    with open(csv_path, "r") as fin:
        reader = csv.reader(fin, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL)
        # Skip header
        next(reader)
        for row in reader:
            url = row[-1]
            urls.append(url)
    return urls


def parse_html_multiprocess(form_dir, parsed_form_dir, overwrite=False):
    """parse html with multiprocess
    Args:
        form_dir (str)
    Returns:
        parsed_form_dir (str)
    """
    # Create directory
    os.makedirs(parsed_form_dir, exist_ok=True)

    # Prepare argument
    form_paths = sorted(glob(os.path.join(form_dir, "*.txt")))
    parsed_form_paths = []
    for form_path in form_paths:
        form_name = os.path.basename(form_path)
        parsed_form_path = os.path.join(parsed_form_dir, form_name)
        parsed_form_paths.append(parsed_form_path)

    # Multiprocess
    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
        for form_path, parsed_form_path in zip(form_paths, parsed_form_paths):
            executor.submit(parse_html, form_path, parsed_form_path, overwrite)


def parse_html(input_file, output_file, overwrite=False):
    """Parses text from html with BeautifulSoup
    Args:
        input_file (str)
        output_file (str)
    """
    if not overwrite and os.path.exists(output_file):
        print("{} already exists.  Skipping parse html...".format(output_file))
        return

    print("Parsing html {}".format(input_file))
    with open(input_file, "r") as fin:
        content = fin.read()
    # Parse html with BeautifulSoup
    soup = BeautifulSoup(content, "html.parser")
    text = soup.get_text("\n")
    write_content(text, output_file)
    # Log message
    print("Write to {}".format(output_file))


def normalize_text(text):
    """Normalize Text"""
    text = unicodedata.normalize("NFKD", text)  # Normalize
    text = "\n".join(text.splitlines())  # Unicode break lines

    # Convert to upper
    text = text.upper()  # Convert to upper

    # Take care of breaklines & whitespaces combinations due to beautifulsoup parsing
    text = re.sub(r"[ ]+\n", "\n", text)
    text = re.sub(r"\n[ ]+", "\n", text)
    text = re.sub(r"\n+", "\n", text)

    # To find MDA section, reformat item headers
    text = text.replace("\n.\n", ".\n")  # Move Period to beginning

    text = text.replace("\nI\nTEM", "\nITEM")
    text = text.replace("\nITEM\n", "\nITEM ")
    text = text.replace("\nITEM  ", "\nITEM ")

    text = text.replace(":\n", ".\n")

    # Math symbols for clearer looks
    text = text.replace("$\n", "$")
    text = text.replace("\n%", "%")

    # Reformat
    text = text.replace("\n", "\n\n")  # Reformat by additional breakline

    return text


def parse_mda_multiprocess(form_dir: str, mda_dir: str, overwrite: bool = False):
    """Parse MDA section from forms with multiprocess
    Args:
        form_dir (str)
        mda_dir (str)
    """
    # Create output directory
    os.makedirs(mda_dir, exist_ok=True)

    # Prepare arguments
    form_paths = sorted(glob(os.path.join(form_dir, "*")))
    mda_paths = []
    for form_path in form_paths:
        form_name = os.path.basename(form_path)
        root, _ = os.path.splitext(form_name)
        mda_path = os.path.join(mda_dir, "{}.mda".format(root))
        mda_paths.append(mda_path)

    # Multiprocess
    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
        for form_path, mda_path in zip(form_paths, mda_paths):
            executor.submit(parse_mda, form_path, mda_path, overwrite)


def parse_mda(form_path, mda_path, overwrite=False):
    """Reads form and parses mda
    Args:
        form_path (str)
        mda_path (str)
    """
    if not overwrite and os.path.exists(mda_path):
        print("{} already exists.  Skipping parse mda...".format(mda_path))
        return
    # Read
    print("Parse MDA {}".format(form_path))
    with open(form_path, "r") as fin:
        text = fin.read()

    # Normalize text here
    text = normalize_text(text)

    # Parse MDA
    mda, end = find_mda_from_text(text)
    # Parse second time if first parse results in index
    if mda and len(mda.encode("utf-8")) < 1000:
        mda, _ = find_mda_from_text(text, start=end)

    if mda:
        print("Write MDA to {}".format(mda_path))
        write_content(mda, mda_path)
    else:
        print("Parse MDA failed {}".format(form_path))


def find_mda_from_text(text, start=0):
    """Find MDA section from normalized text
    Args:
        text (str)s
    """
    debug = False

    mda = ""
    end = 0

    # Define start & end signal for parsing
    item7_begins = ["\nITEM 7.", "\nITEM 7 –", "\nITEM 7:", "\nITEM 7 ", "\nITEM 7\n"]
    item7_ends = ["\nITEM 7A"]
    if start != 0:
        item7_ends.append("\nITEM 7")  # Case: ITEM 7A does not exist
    item8_begins = ["\nITEM 8"]
    """
    Parsing code section
    """
    text = text[start:]

    # Get begin
    for item7 in item7_begins:
        begin = text.find(item7)
        if debug:
            print(item7, begin)
        if begin != -1:
            break

    if begin != -1:  # Begin found
        for item7A in item7_ends:
            end = text.find(item7A, begin + 1)
            if debug:
                print(item7A, end)
            if end != -1:
                break

        if end == -1:  # ITEM 7A does not exist
            for item8 in item8_begins:
                end = text.find(item8, begin + 1)
                if debug:
                    print(item8, end)
                if end != -1:
                    break

        # Get MDA
        if end > begin:
            mda = text[begin:end].strip()
        else:
            end = 0

    return mda, end


if __name__ == "__main__":
    main()
