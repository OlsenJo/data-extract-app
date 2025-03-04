

import time 
import logging
import requests
from typing import Optional
from urllib.parse import quote

from config import Config

logger = logging.getLogger(__name__)

class CSVDowloader:

    def __init__(self):
        self.config = Config()
        self.session = requests.Session()

    def build_url(self, gas_day: str, cycle: int) -> str:

        params = self.config.URL_PARAMS.copy()

        params["gasDay"] = quote(gas_day)
        params["cycle"] = quote(cycle)

        query_string = "&".join([f"{k}={v}" for k, v in params.items()])

        return f"{self.config.BASE_URL}?{query_string}"
    
    