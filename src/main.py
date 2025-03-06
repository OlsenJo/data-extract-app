import os
import logging
import datetime
import pandas as pd
from tabulate import tabulate
from tqdm import tqdm

from config import Config
from downloader import CSVDownloader
from parser import CSVParser
from database import Database

# Print pipeline explanation when script is run
def print_pipeline_explanation():
    explanation = """
=======================================================================
NATURAL GAS SHIPMENT DATA PIPELINE
=======================================================================

This pipeline will perform the following operations:

1. Download CSV files from Energy Transfer's website containing natural 
   gas shipment data for the last 3 days (all available cycles)
   
2. Store these CSV files in a temporary folder (configurable in .env)

3. Parse and validate the data from these CSV files

4. Display a summary of all collected data for your review

5. Ask for your confirmation before proceeding with database insertion

6. If confirmed, insert the data into the PostgreSQL database

The process may take a few minutes depending on the amount of data and 
your internet connection speed. You'll see progress bars for each step.

=======================================================================
"""
    print(explanation)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_last_few_days(days=3):
    today = datetime.date.today()
    
    date_list = []
    
    # Add the last 'days' days to the list
    for i in range(1, days + 1):
        date_list.append(today - datetime.timedelta(days=i))
    
    return date_list

def format_date_for_url(date):
    return date.strftime("%m/%d/%Y")

def show_data_summary(records):

    if not records:
        print("No records to display.")
        return
    
    df = pd.DataFrame(records)
    
    # Group by gas_day and cycle
    grouped = df.groupby(['gas_day', 'cycle']).size().reset_index(name='record_count')
    
    # Display summary
    print("\n" + "="*80)
    print("DATA SUMMARY")
    print("="*80)
    print(f"Total records to be inserted: {len(records)}")
    print(f"Date range: {df['gas_day'].min()} to {df['gas_day'].max()}")
    print(f"Number of unique locations: {df['loc'].nunique()}")
    print("\nRecords by date and cycle:")
    print(tabulate(grouped, headers='keys', tablefmt='grid'))
    
    # Display sample records
    print("\nSample records (first 5):")
    sample_columns = ['loc', 'loc_name', 'gas_day', 'cycle', 'oper_capacity', 'operationally_available']
    sample_df = df[sample_columns].head(5)
    print(tabulate(sample_df, headers='keys', tablefmt='grid'))
    print("="*80)

def ask_user_to_continue():
 
    while True:
        answer = input("\nDo you want to proceed with database insertion? (yes/no): ").strip().lower()
        
        if answer == 'yes':
            return True
        elif answer == 'no':
            return False
        else:
            print("Please enter 'yes' or 'no'.")

def main():
    logger.info("Starting natural gas shipment data pipeline")
    
    print_pipeline_explanation()
    
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
        
        # Get dates for the last 3 days
        dates = get_last_few_days(days=3)
        logger.info(f"Processing data for dates: {[d.strftime('%Y-%m-%d') for d in dates]}")
        
        # List to store all records
        all_records = []
        
        for date in tqdm(dates, desc="Processing dates"):
            formatted_date = format_date_for_url(date)
            
            # Process each cycle (usually 1-5)
            for cycle in tqdm(range(1, 6), desc=f"Processing cycles for {date.strftime('%Y-%m-%d')}"):
                try:
                    logger.info(f"Processing date: {date.strftime('%Y-%m-%d')}, cycle: {cycle}")
                    
                    # Download the CSV
                    csv_data, temp_file_path = downloader.download_csv(
                        gas_day=formatted_date,
                        cycle=cycle
                    )
                    
                    if not csv_data:
                        logger.warning(f"No data available for date: {date.strftime('%Y-%m-%d')}, cycle: {cycle}")
                        continue
                    
                    records = parser.parse_csv(csv_data)
                    
                    if not records:
                        logger.warning(f"No valid records found for date: {date.strftime('%Y-%m-%d')}, cycle: {cycle}")
                        continue
                    
                    for record in records:
                        record['gas_day'] = date.isoformat()
                        record['cycle'] = cycle
                    
                    all_records.extend(records)
                    
                    if temp_file_path and not config.KEEP_TEMP_FILES:
                        downloader.cleanup(temp_file_path)
                    
                except Exception as e:
                    logger.error(f"Error processing date: {date.strftime('%Y-%m-%d')}, cycle: {cycle}: {str(e)}")
        
        if all_records:
            show_data_summary(all_records)
            
            if ask_user_to_continue():
                # Insert into database in small groups to avoid errors
                inserted_count = db.insert_in_small_groups(all_records, group_size=100)
                logger.info(f"Inserted {inserted_count} records into the database")
                print(f"\nSuccessfully inserted {inserted_count} records into the database.")
            else:
                logger.info("User chose not to insert data into the database")
                print("\nDatabase insertion cancelled by user.")
        else:
            logger.warning("No records to insert")
            print("\nNo records found to insert into the database.")
        
        if not config.KEEP_TEMP_FILES:
            downloader.cleanup()
        
        logger.info("Pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    finally:
        if 'db' in locals():
            db.close()

if __name__ == "__main__":
    main()

