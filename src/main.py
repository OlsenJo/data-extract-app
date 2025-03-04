
import os 
import logging
import datetime
from typing import List, Tuple

from config import Config
from downloader import CSVDownloader
from parser import CSVParser
from database import Database


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_date_range(days: int = 3) -> List[datetime.date]:

    today = datetime.date.today()
    return [(today - datetime.timedelta(days=i)) for i in range(1,days + 1)]

def format_url_date(date: datetime.date) -> str:
    return date.strftime("%m/%d/%Y")

def main():

    logger.info("Starting extaction data pipeline")


    try:
        config = Config()
        downloader = CSVDownloader()
        parser = CSVParser()
        db = Database(
            host=config.DB_HOST,
            port=config.DB_PORT,
            database=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD
        )

        db.initialize_database()

        dates = get_date_range(days=3)
        logger.info(f"Currently Processing data for dates: {[d.strftime('%Y-%m-%d') for d in dates]}")

        for date in dates:
            formatted_date = format_url_date(date)
            
            # Process each cycle (typically 1-5)
            for cycle in range(1, 6):
                try:
                    logger.info(f"Processing date: {date.strftime('%Y-%m-%d')}, cycle: {cycle}")
                    
                    # Download CSV
                    csv_data, temp_file_path = downloader.download_csv(
                        gas_day=formatted_date,
                        cycle=cycle
                    )
                    
                    if not csv_data:
                        logger.warning(f"No data available for date: {date.strftime('%Y-%m-%d')}, cycle: {cycle}")
                        continue
                    
                    # Parse and validate CSV
                    records = parser.parse_csv(csv_data)
                    
                    if not records:
                        logger.warning(f"No valid records found for date: {date.strftime('%Y-%m-%d')}, cycle: {cycle}")
                        continue
                    
                    # Add date and cycle information to each record
                    for record in records:
                        record['gas_day'] = date.isoformat()
                        record['cycle'] = cycle
                    
                    # Insert into database
                    inserted_count = db.insert_records(records)
                    logger.info(f"Inserted {inserted_count} records for date: {date.strftime('%Y-%m-%d')}, cycle: {cycle}")
                    
                    # Clean up temporary file if configured
                    if temp_file_path and not config.KEEP_TEMP_FILES:
                        downloader.cleanup(temp_file_path)
                    
                except Exception as e:
                    logger.error(f"Error processing date: {date.strftime('%Y-%m-%d')}, cycle: {cycle}: {str(e)}")
        
        logger.info("Pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    finally:
        # Ensure database connection is closed
        if 'db' in locals():
            db.close()

if __name__ == "__main__":
    main()

