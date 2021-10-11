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

from edgar.constants import SUPPORTED_FORM_TYPES
from edgar.download import download_file, form_url_iterator
from edgar.util import write_content


def create_parser():
    """Argument Parser"""
    # TODO: add description and example_usage
    description = "Script to download Edgar index files"
    example_usage = ""
    parser = argparse.ArgumentParser(description=description, epilog=example_usage)
    parser.add_argument("-i", "--index-dir", type=str, required=True, help="directory to input indices")
    parser.add_argument("-o", "--output-dir", type=str, required=True, help="directory to save forms")
    parser.add_argument("-f", "--form-type", type=str, default='10K', choices=SUPPORTED_FORM_TYPES, help="supported form types")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="If True, overwrites downloads and processed files.",
    )
    parser.add_argument("--dry-run", action="store_true", help="list files to download")
    return parser


def main():
    # Parse arguments
    parser = create_parser()
    args = parser.parse_args()

    # create directory and download forms
    os.makedirs(args.output_dir, exist_ok=True)
    for form_url, cik, form_name in form_url_iterator(args.input_dir, args.form_type):
        cik_dir = os.path.join(args.output_dir, cik)
        os.makedirs(cik_dir, exist_ok=True)
        download_path = os.path.join(cik_dir, form_name)
        print(f"Downloading from {form_url}")
        if args.dry_run:
            continue
        download_file(form_url, download_path, args.overwrite)

    print(f"Output dir: {args.output_dir}")


if __name__ == "__main__":
    main()
