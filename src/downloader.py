

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
    
    def download_csv(self, gas_day: str, cycle:int) -> Optional[str]:
        url = self.build_url(gas_day, cycle)
        logger.info(f"Downloading  CSV from : {url}")

        for attempt in range(1, self.config.MAX_RETRIES + 1):
            try: 
                response = self.session.get(
                    url,
                    timeout=self.config.REQUEST_TIMEOUT
                )

                if response.status_code ==200:
                    content_type = response.headers.get('Content-Type', '')
                    if 'text/csv' in content_type or 'application/csv' in content_type:
                        return response.text
                    elif 'text/html' in content_type and 'No data found' in response.text:
                        logger.warning(f"No data Found for gas day: {gas_day}, cycle: {cycle}")
                        return None
                    else:
                        logger.warning(
                            f"Unexpected content type: {content_type} for gas day: {gas_day}, cycle: {cycle}"
                        )
                        return None
                else: 
                    logger.warning(
                        f"Failed to download CSV (attempt {attempt}/{self.config.MAX_RETRIES}): "
                        f"Status code {response.status_code}"
                    )
            except requests.RequestException as e:
                logger.warning(
                    f"Request exception (attempt {attempt}/{self.config.MAX_RETRIES}): {str(e)}"
                )
            
            if attempt < self.config.MAX_RETRIES:
                time.sleep(self.config.RETRY_DELAY)
        
        logger.error(f"Failed to download CSV after {self.config.MAX_RETRIES} attempts")
        return None
    