import os
import time
import logging
import requests
from typing import Optional, Tuple
from urllib.parse import quote

from config import Config

logger = logging.getLogger(__name__)

class CSVDownloader:    
    def __init__(self):
        self.config = Config()
        self.session = requests.Session()
        
        # Create temporary folder if it doesn't exist
        os.makedirs(self.config.TEMP_FOLDER, exist_ok=True)
        logger.info(f"Temporary folder for CSV files: {self.config.TEMP_FOLDER}")
    
    def build_url(self, gas_day: str, cycle: int) -> str:
      
        params = self.config.URL_PARAMS.copy()
        
        params["gasDay"] = quote(gas_day)
        params["cycle"] = str(cycle)
        
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        
        return f"{self.config.BASE_URL}?{query_string}"
    
    def get_temp_file_path(self, gas_day: str, cycle: int) -> str:
       
        # Convert gas_day from MM/DD/YYYY to YYYY-MM-DD for the filename
        parts = gas_day.split('/')
        if len(parts) == 3:
            formatted_date = f"{parts[2]}-{parts[0]}-{parts[1]}"
        else:
            formatted_date = gas_day.replace('/', '-')
        
        return os.path.join(self.config.TEMP_FOLDER, f"gas_data_{formatted_date}_cycle_{cycle}.csv")
    
    def download_csv(self, gas_day: str, cycle: int) -> Tuple[Optional[str], Optional[str]]:
       
        url = self.build_url(gas_day, cycle)
        temp_file_path = self.get_temp_file_path(gas_day, cycle)
        
        # Check if the file already exists
        if os.path.exists(temp_file_path):
            logger.info(f"CSV file already exists at: {temp_file_path}")
            with open(temp_file_path, 'r', encoding='utf-8') as file:
                return file.read(), temp_file_path
        
        logger.info(f"Downloading CSV from: {url}")
        
        for attempt in range(1, self.config.MAX_RETRIES + 1):
            try:
                response = self.session.get(
                    url, 
                    timeout=self.config.REQUEST_TIMEOUT
                )
                
                # Check if the request was successful
                if response.status_code == 200:
                    content_type = response.headers.get('Content-Type', '')
                    if 'text/csv' in content_type or 'application/csv' in content_type:
                        # Save CSV to file
                        with open(temp_file_path, 'w', encoding='utf-8') as file:
                            file.write(response.text)
                        logger.info(f"Saved CSV to: {temp_file_path}")
                        return response.text, temp_file_path
                    elif 'text/html' in content_type and 'No data found' in response.text:
                        logger.warning(f"No data found for gas day: {gas_day}, cycle: {cycle}")
                        return None, None
                    else:
                        logger.warning(
                            f"Unexpected content type: {content_type} for gas day: {gas_day}, cycle: {cycle}"
                        )
                        return None, None
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
        return None, None
    
    def cleanup(self, file_path: str = None):
      
        if not self.config.KEEP_TEMP_FILES:
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    logger.info(f"Deleted temporary file: {file_path}")
                except Exception as e:
                    logger.warning(f"Failed to delete temporary file {file_path}: {str(e)}")
            elif file_path is None:
                try:
                    for filename in os.listdir(self.config.TEMP_FOLDER):
                        file_path = os.path.join(self.config.TEMP_FOLDER, filename)
                        if os.path.isfile(file_path):
                            os.remove(file_path)
                    logger.info(f"Cleaned up all files in {self.config.TEMP_FOLDER}")
                except Exception as e:
                    logger.warning(f"Failed to clean up temporary folder: {str(e)}")

