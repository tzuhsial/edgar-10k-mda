"""
Functions related to form parsing

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

from bs4 import BeautifulSoup

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
    """ Parses text from html with BeautifulSoup
    Args:
        input_file (str)
        output_file (str)
    """
    if not overwrite and os.path.exists(output_file):
        print("{} already exists.  Skipping parse html...".format(output_file))
        return

    print("Parsing html {}".format(input_file))
    with open(input_file, 'r') as fin:
        content = fin.read()
    # Parse html with BeautifulSoup
    soup = BeautifulSoup(content, "html.parser")
    text = soup.get_text("\n")
    write_content(text, output_file)
    # Log message
    print("Write to {}".format(output_file))


def normalize_text(text):
    """Normalize Text
    """
    text = unicodedata.normalize("NFKD", text)  # Normalize
    text = '\n'.join(text.splitlines())  # Unicode break lines

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


def parse_mda_multiprocess(form_dir: str, mda_dir: str, overwrite: bool = False):
    """ Parse MDA section from forms with multiprocess
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
        mda_path = os.path.join(mda_dir, '{}.mda'.format(root))
        mda_paths.append(mda_path)

    # Multiprocess
    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
        for form_path, mda_path in zip(form_paths, mda_paths):
            executor.submit(parse_mda, form_path, mda_path, overwrite)


def parse_mda(form_path, mda_path, overwrite=False):
    """ Reads form and parses mda
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
    if mda and len(mda.encode('utf-8')) < 1000:
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