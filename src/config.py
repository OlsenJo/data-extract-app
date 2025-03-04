import os
from dotenv import load_dotenv

load_dotenv

class Config:
    
    BASE_URL = "https://twtransfer.energytransfer.com/ipost/capacity/operationally-available"

    URL_PARAMS = {

        "f": "csv",
        "extension": "csv",
        "asset": "TW",
        "searchType": "NOM",
        "searchString": "",
        "locType": "ALL",
        "locZone": "ALL"

    }

    # Database configuration
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "gas_shipments")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

    # Request timeout in seconds
    REQUEST_TIMEOUT = 30
    
    # Maximum retries for HTTP requests
    MAX_RETRIES = 3
    
    # Retry delay in seconds
    RETRY_DELAY = 5
