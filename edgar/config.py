# 10 requests per second limit: https://www.sec.gov/developer
HEADERS = {"User-Agent": None, # TODO: change this to your email
           "Accept-Encoding": "gzip, deflate",
           "Host": "www.sec.gov"}
REQUESTS_PER_SECOND = 10