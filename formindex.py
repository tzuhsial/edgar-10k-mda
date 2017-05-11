import argparse
from collections import namedtuple
import csv
import itertools
import os

import requests

SEC_GOV_URL = 'http://www.sec.gov/Archives'
FORM_INDEX_URL = os.path.join(SEC_GOV_URL,'edgar','full-index','{}','QTR{}','form.idx')
IndexRecord = namedtuple("IndexRecord",["form_type","company_name","cik","date_filed","filename"])

class FormIndex(object):
    def __init__(self):
        self.formrecords = []
        self.index_dir = None

    def retrieve(self, index_dir, year_start, year_end):
        # Create Index directory
        self.index_dir = index_dir
        if not os.path.exists(index_dir):
            os.makedirs(index_dir)

        # Retrieve raw index files
        for year, qtr in itertools.product(range(args.year_start,args.year_end+1),range(1,5)):
            form_idx = "year{}_qtr{}.index".format(year,qtr)
            form_idx_path = os.path.join(self.index_dir,form_idx)
            self.download(form_idx_path, year, qtr)
            self.extract(form_idx_path)

    def download(self, form_idx_path, year, qtr):
        if os.path.exists(form_idx_path):
            print("Download skipped: year {}, qtr {}".format(year,qtr))
            return

        # Download and save to cached directory
        print("Downloading year {}, qtr {}".format(year,qtr))

        index_url = FORM_INDEX_URL.format(year,qtr)
        resp = requests.get(index_url)

        with open(form_idx_path,'wb') as fout:
            fout.write(resp.content)

    def extract(self, form_idx_path):
        # Parse row to record
        def parse_row_to_record(row, fields_begin):
            record = []

            for begin, end in zip(fields_begin[:],fields_begin[1:] + [len(row)] ):
                field = row[begin:end].rstrip()
                field = field.strip('\"')
                record.append(field)

            return record

        # Read and parse
        print("Extracting from {}".format(form_idx_path))

        with open(form_idx_path,'rb') as fin:
            # If arrived at 10-K section of forms
            arrived = False

            for row in fin.readlines():
                row = row.decode('ascii')
                if row.startswith("Form Type"):
                    fields_begin = [ row.find("Form Type"),
                                     row.find("Company Name"),
                                     row.find('CIK'),
                                     row.find('Date Filed'),
                                     row.find("File Name") ]

                elif row.startswith("10-K "):
                    arrived = True
                    rec = parse_row_to_record(row,fields_begin)
                    self.formrecords.append(IndexRecord(*rec))

                elif arrived == True:
                    break

    def save(self, index_path):
        print("Saving records to {}".format(index_path))
        with open(index_path,'w') as fout:
            writer = csv.writer(fout,delimiter=',',quotechar='\"',quoting=csv.QUOTE_ALL)
            for rec in self.formrecords:
                writer.writerow( tuple(rec) )

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Download EDGAR form 10k index")
    parser.add_argument('--year_start',type=int,default=2014)
    parser.add_argument('--year_end',type=int,default=2016)
    parser.add_argument('--index_dir',type=str,default='./data/index')
    parser.add_argument('--out_file',type=str,default="")
    args = parser.parse_args()

    index_path = ( "year{}-{}.10k.index".format(args.year_start,args.year_end) if not args.out_file else args.out_file )

    formindex = FormIndex()
    formindex.retrieve(index_dir=args.index_dir, year_start=args.year_start, year_end=args.year_end)
    formindex.save(index_path=index_path)
