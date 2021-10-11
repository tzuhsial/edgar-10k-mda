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

from edgar.download import index_url_iterator, download_file
from edgar.util import write_content


def create_parser():
    """Argument Parser"""
    # TODO: add description and example_usage
    description = "Script to download Edgar index files"
    example_usage = ""
    parser = argparse.ArgumentParser(description=description, epilog=example_usage)
    parser.add_argument(
        "-s", "--start-year", type=int, required=True, help="starting year"
    )
    parser.add_argument("-e", "--end-year", type=int, required=True, help="ending year")
    parser.add_argument(
        "-q",
        "--quarters",
        type=int,
        nargs="+",
        default=[1, 2, 3, 4],
        help="quarters to download for start to end years, defaults to [1, 2, 3, 4]",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=str,
        required=True,
        help="output directory to save indices",
    )
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

    # create directory and download indices
    os.makedirs(args.output_dir, exist_ok=True)
    for index_url, index_name in index_url_iterator(
        args.start_year, args.end_year, args.quarters
    ):
        download_path = os.path.join(args.output_dir, index_name)
        print(f"Downloading from {index_url}")
        if args.dry_run:
            continue
        download_file(index_url, download_path, args.overwrite)

    print(f"Output dir: {args.output_dir}")


if __name__ == "__main__":
    main()
