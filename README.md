# edgar-10k-mda

This repo contains some python code I used to download form10k filings  from [EDGAR database](https://www.sec.gov/edgar.shtml), 
and then extract the MDA section from the downloaded form10k filings(in a brute force way).

### Getting Started 

The whole workflow contains 3 steps
- Step 1. Download index of the form 10k filings
- Step 2. Using the index from Step 1., download the form 10k filings(html) and parse text out of html using [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)
- Step 3. Parse the MDA section of form10k text from Step 2.


Below are the commands with arguments to run each step 

Step 1. Download index of the form 10k filings with start year and end year and save to 'index-10-path'. Raw index files with also be saved to '--index-dir=./index'.
```bash
python edgar.py download index --year-start=2016 --year-end=2016 --index-dir=./index  --index-10k-path=index.10k.csv
```

Step 2. Download Form 10k filings using '--index-10k-path' from Step 1, parse with BeautifulSoup and save to '--10k-dir'
```bash
python edgar.py download 10k --index-10k-path=./index.10k.csv --10k-dir=./form10k
```

Step 3. Parse the MDA section of the downloaded text in '--10k-dir' and save to '--mda-dir'
```bash
python edgar.py extract mda --10k-dir=./form10k --mda-dir=./mda
```

### Installation

I used python3.6
```bash
#python36
pip install -r requirements.txt
```
