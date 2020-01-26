# edgar-10k-mda

This repo contains some python code I used to download form10k filings  from [EDGAR database](https://www.sec.gov/edgar.shtml), 
and then extract the MDA section from the downloaded form10k filings heuristically

### Usage

Specify the starting year and end year and the directory to save outputs.
By default, indices, forms and mdas will be saved to `./data`

```bash
# Downloads and parses MDA section from 2016 to 2016
python edgar.py --start_year=2016 --end_year=2016
```

### Notes

- MDA section is parsed heuristically, and may not work for all forms. You'll probably need to modify the `find_mda_from_text` function for coverage.
- You also might need to modify `normalize_text` function for MDA parsing.

### Workflow

The code runs the extraction in the following steps
1. Download indices for form 10k to `./data/index`
2. Combines all indices into a single csv `./data/index/combined.csv`
3. From Step2 combined csv, downloads all form 10k to `./data/form10k`
4. Parses the html forms with BeautifulSoup to `./data/form10k.parsed`
5. Parses MDA section to `./data/mda`

### Installation

I used python3.6
```bash
#python36
pip install -r requirements.txt
```
