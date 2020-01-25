"""Edgar 10k MDA 
Usage:
    edgar.py download index [options]
    edgar.py download 10k [options]
    edgar.py extract mda [options]

Options:
    --index-dir=<file>          Directory to save index files [default: ./index]
    --index-10k-path=<file>     CSV file to store 10k indices [default: ./index.10k.csv]
    --10k-dir=<file>            Directory to save 10k files [default: ./form10k]
    --mda-dir=<file>            Directory to save mda files [default: ./mda]
    --year-start=<int>          Starting year for download index [default: 2016]
    --year-end=<int>            Ending year for download index [default: 2016]
"""
import csv
import itertools
import os
import re
import unicodedata
from collections import namedtuple
from glob import glob

import requests
from bs4 import BeautifulSoup
from docopt import docopt
from tqdm import tqdm

SEC_GOV_URL = 'https://www.sec.gov/Archives'
FORM_INDEX_URL = os.path.join(
    SEC_GOV_URL, 'edgar', 'full-index', '{}', 'QTR{}', 'form.idx')
IndexRecord = namedtuple(
    "IndexRecord", ["form_type", "company_name", "cik", "date_filed", "filename"])


def download_and_extract_index(opt):
    index_dir = opt["--index-dir"]
    os.makedirs(index_dir, exist_ok=True)

    year_start = int(opt["--year-start"])
    year_end = int(opt["--year-end"])

    for year, qtr in itertools.product(range(year_start, year_end+1), range(1, 5)):

        index_url = FORM_INDEX_URL.format(year, qtr)
        try:
            print("request index - {}".format(index_url))
            res = requests.get(index_url)
            form_idx = "year{}_qtr{}.index".format(year, qtr)
            form_idx_path = os.path.join(index_dir, form_idx)

            print("writing index to {}".format(form_idx_path))
            with open(form_idx_path, 'w') as fout:
                fout.write(res.text)
        except Exception as e:
            print(e)

    def parse_row_to_record(row, fields_begin):
        record = []

        for begin, end in zip(fields_begin[:], fields_begin[1:] + [len(row)]):
            field = row[begin:end].rstrip()
            field = field.strip('\"')
            record.append(field)

        return record

    records = []
    for index_file in sorted(glob(os.path.join(index_dir, "*.index"))):
        print("Extracting 10k records from index {}".format(index_file))

        with open(index_file, 'r') as fin:
            # If arrived at 10-K section of forms
            arrived = False

            for row in fin.readlines():
                if row.startswith("Form Type"):
                    fields_begin = [row.find("Form Type"),
                                    row.find("Company Name"),
                                    row.find('CIK'),
                                    row.find('Date Filed'),
                                    row.find("File Name")]

                elif row.startswith("10-K "):
                    arrived = True
                    rec = parse_row_to_record(row, fields_begin)
                    records.append(IndexRecord(*rec))

                elif arrived == True:
                    break

    index_10k_path = opt["--index-10k-path"]
    with open(index_10k_path, 'w') as fout:
        writer = csv.writer(fout, delimiter=',',
                            quotechar='\"', quoting=csv.QUOTE_ALL)
        for rec in records:
            writer.writerow(tuple(rec))


def download_10k(opt):
    """Downloads 10k HTML and saves only text 
    """
    index_10k_path = opt["--index-10k-path"]
    if not os.path.exists(index_10k_path):
        raise OSError("directory not found: {}".format(index_10k_path))
    form10k_dir = opt["--10k-dir"]
    os.makedirs(form10k_dir, exist_ok=True)

    with open(index_10k_path, 'r') as fin:
        reader = csv.reader(
            fin, delimiter=',', quotechar='\"', quoting=csv.QUOTE_ALL)

        for row in reader:
            _, _, _, _, filename = row
            url = os.path.join(SEC_GOV_URL, filename).replace(
                "\\", "/")
            print('request 10k html - {}'.format(url))

            try:
                res = requests.get(url)
                soup = BeautifulSoup(res.content, "html.parser")
                text = soup.get_text("\n")
                fname = '_'.join(url.split('/')[-2:])
                text_path = os.path.join(form10k_dir, fname)
                print("writing 10k text to {}".format(text_path))
                with open(text_path, 'w') as fout:
                    fout.write(text)
            except Exception as e:
                print(e)


def extract_mda(opt):
    form10k_dir = opt["--10k-dir"]
    if not os.path.exists(form10k_dir):
        raise OSError("Directory not found: {}".format(form10k_dir))
    mda_dir = opt["--mda-dir"]
    os.makedirs(mda_dir, exist_ok=True)

    for form10k_file in tqdm(sorted(glob(os.path.join(form10k_dir, "*.txt")))):
        print("extracting mda from form10k file {}".format(form10k_file))

        # Read form 10k
        with open(form10k_file, 'r') as fin:
            text = fin.read()

        # Normalize
        text = normalize_text(text)

        # Find MDA section
        mda, end = parse_mda(text)
        # Parse second time if first parse results in index
        if mda and len(mda.encode('utf-8')) < 1000:
            mda, _ = parse_mda(text, start=end)

        if mda:
            filename = os.path.basename(form10k_file)
            name, _ = os.path.splitext(filename)
            mda_path = os.path.join(mda_dir, name + ".mda")
            print("writing mda to {}".format(mda_path))
            with open(mda_path, 'w') as fout:
                fout.write(mda)
        else:
            print("parse_mda failed for - {}".format(form10k_file))


def normalize_text(text):
    """Nomralize Text
    """
    text = unicodedata.normalize("NFKD", text)  # Normalize
    text = '\n'.join(
        text.splitlines())  # Let python take care of unicode break lines

    # Convert to upper
    text = text.upper()  # Convert to upper

    # Take care of breaklines & whitespaces combinations due to beautifulsoup parsing
    text = re.sub(r'[ ]+\n', '\n', text)
    text = re.sub(r'\n[ ]+', '\n', text)
    text = re.sub(r'\n+', '\n', text)

    # To find MDA section, reformat item headers
    text = text.replace('\n.\n', '.\n')  # Move Period to beginning

    text = text.replace('\nI\nTEM', '\nITEM')
    text = text.replace('\nITEM\n', '\nITEM ')
    text = text.replace('\nITEM  ', '\nITEM ')

    text = text.replace(':\n', '.\n')

    # Math symbols for clearer looks
    text = text.replace('$\n', '$')
    text = text.replace('\n%', '%')

    # Reformat
    text = text.replace('\n', '\n\n')  # Reformat by additional breakline

    return text


def parse_mda(text, start=0):
    debug = False
    """Parse normalized text 
    """

    mda = ""
    end = 0
    """
        Parsing Rules
    """

    # Define start & end signal for parsing
    item7_begins = [
        '\nITEM 7.', '\nITEM 7 –', '\nITEM 7:', '\nITEM 7 ', '\nITEM 7\n'
    ]
    item7_ends = ['\nITEM 7A']
    if start != 0:
        item7_ends.append('\nITEM 7')  # Case: ITEM 7A does not exist
    item8_begins = ['\nITEM 8']
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
    opt = docopt(__doc__)
    print(opt)
    if opt["download"] and opt["index"]:
        download_and_extract_index(opt)
    elif opt["download"] and opt["10k"]:
        download_10k(opt)
    elif opt["extract"] and opt["mda"]:
        extract_mda(opt)
