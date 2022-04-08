from urllib.parse import urljoin

# Constants
SEC_GOV_URL = 'https://www.sec.gov/Archives/'
FORM_INDEX_URL_TEMPLATE = urljoin(SEC_GOV_URL, 'edgar/full-index/{}/QTR{}/form.idx')

# Supported form types
SUPPORTED_FORM_TYPES = ["10-K"]

# Used to combine form 10k index files. Adds URL column for lookup
INDEX_HEADERS = ["Form Type", "Company Name",
                 "CIK", "Date Filed", "File Name", "Url"]
