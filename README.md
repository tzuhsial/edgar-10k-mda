# edgar-10k-mda

Here are the scripts I used to download then extract the MDA section ofForm 10k filings from [EDGAR database](https://www.sec.gov/edgar/).

## Workflow

1. Download the index file of form 10k filings, raw index files will be saved to './data/index', and create an aggregated index file './year2016-2016.10k.index'
```
python formindex.py --year_start 2016 --year_end 2016 --index_dir ./data/index --out_file ./year2016-2016.10k.index
```

2. Download Form 10k filings using the previously generated index file and save to text directory './data/txt'
  ```
  python form10k.py --index_path ./year2016-2016.10k.index --txt_dir ./data/txt
  ```
3. Parse the MDA section of the downloaded text and save to mda directory './data/mda'
```
python mdaparser.py --txt_dir ./data/txt --mda_dir ./data/mda
```

### Installing
python: 3.5
```
pip install -r requirements.txt
```
