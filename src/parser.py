import csv
import logging
import io
from typing import List, Dict, Any, Optional, Union

logger = logging.getLogger(__name__)

class CSVParser:
    
    def __init__(self):
        """Initialize the parser."""
        pass
    
    def parse_csv(self, csv_data: str) -> List[Dict[str, Any]]:
       
        logger.info("Parsing CSV data")
        
        # Create a CSV reader
        csv_file = io.StringIO(csv_data)
        reader = csv.DictReader(csv_file)
        
        # Process each row
        records = []
        for i, row in enumerate(reader, start=1):
            try:
                # Clean and validate the record
                record = self._clean_record(row)
                
                if record:
                    records.append(record)
                else:
                    logger.warning(f"Skipping invalid record at row {i}")
            except Exception as e:
                logger.error(f"Error processing row {i}: {str(e)}")
        
        logger.info(f"Parsed {len(records)} valid records from CSV")
        return records
    
    def _clean_record(self, row: Dict[str, str]) -> Optional[Dict[str, Any]]:
       
        # Create a new record with cleaned data
        record = {}
        
        # Process each field
        try:
            # Required fields
            if not row.get('Loc'):
                logger.warning("Missing required field: Loc")
                return None
            
            record['loc'] = row.get('Loc', '').strip()
            record['loc_zone'] = row.get('Loc Zone', '').strip()
            record['loc_name'] = row.get('Loc Name', '').strip()
            record['loc_purpose'] = row.get('Loc Purpose', '').strip()
            record['measure_basis'] = row.get('Meas Basis Desc', '').strip()
            
            # Numeric fields
            record['oper_capacity'] = self._parse_numeric(row.get('Oper Capacity', ''))
            record['design_capacity'] = self._parse_numeric(row.get('Design Capacity', ''))
            record['scheduled_qty'] = self._parse_numeric(row.get('Scheduled Qty', ''))
            record['operationally_available'] = self._parse_numeric(row.get('Operationally Available', ''))
            record['total_scheduled'] = self._parse_numeric(row.get('Total Scheduled', ''))
            
            return record
        except Exception as e:
            logger.error(f"Error cleaning record: {str(e)}")
            return None
    
    def _parse_numeric(self, value: str) -> Optional[float]: 
        if not value or value.strip() == '':
            return None
        
        # Remove commas and other non-numeric characters
        cleaned_value = value.replace(',', '').strip()
        
        try:
            return float(cleaned_value)
        except ValueError:
            logger.warning(f"Could not parse numeric value: {value}")
            return None

