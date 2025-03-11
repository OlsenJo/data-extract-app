import csv
import logging
import io
from typing import List, Dict, Any, Optional, Union

logger = logging.getLogger(__name__)

class CSVParser:
    
    def __init__(self):
        self.column_mapping = {
            'Loc': 'loc',
            'Loc Zn': 'loc_zone',
            'Loc Name': 'loc_name',
            'Loc Purp Desc': 'loc_purpose',
            'Loc/QTI': 'loc_qti',
            'Flow Ind': 'flow_ind',
            'DC': 'design_capacity',
            'OPC': 'oper_capacity',
            'TSQ': 'total_scheduled',
            'OAC': 'operationally_available',
            'IT': 'it',
            'Auth Overrun Ind': 'auth_overrun_ind',
            'Nom Cap Exceed Ind': 'nom_cap_exceed_ind',
            'All Qty Avail': 'all_qty_avail',
            'Qty Reason': 'qty_reason'
        }
        
        self.numeric_columns = [
            'DC', 'OPC', 'TSQ', 'OAC', 'IT'
        ]
        
        self.indicator_columns = [
            'Auth Overrun Ind', 'Nom Cap Exceed Ind', 'All Qty Avail'
        ]
    
    def parse_csv(self, csv_data: str) -> List[Dict[str, Any]]:
    
        logger.info("Parsing CSV data")
        
        csv_file = io.StringIO(csv_data)
        reader = csv.DictReader(csv_file)
        
        if reader.fieldnames:
            logger.info(f"CSV headers: {reader.fieldnames}")
            
            missing_headers = [h for h in self.numeric_columns if h not in reader.fieldnames]
            if missing_headers:
                logger.warning(f"Missing expected headers: {missing_headers}")
        
        records = []
        for i, row in enumerate(reader, start=1):
            try:
                record = self._clean_record(row)
                
                if record:
                    records.append(record)
                else:
                    logger.warning(f"Skipping invalid record at row {i}")
            except Exception as e:
                logger.error(f"Error processing row {i}: {str(e)}")
                logger.error(f"Problematic row: {row}")
        
        logger.info(f"Parsed {len(records)} valid records from CSV")
        return records
    
    def _clean_record(self, row: Dict[str, str]) -> Optional[Dict[str, Any]]:
      
        record = {}
        
        try:
            if not row.get('Loc'):
                logger.warning("Missing required field: Loc")
                return None
            
            for csv_col, db_col in self.column_mapping.items():
                if csv_col in row:
                    if csv_col in self.numeric_columns:
                        record[db_col] = self._parse_numeric(row.get(csv_col, ''))
                    elif csv_col in self.indicator_columns:
                        record[db_col] = row.get(csv_col, '').strip()
                    else:
                        record[db_col] = row.get(csv_col, '').strip()
            
            return record
        except Exception as e:
            logger.error(f"Error cleaning record: {str(e)}")
            return None
    
    def _parse_numeric(self, value: str) -> Optional[float]:
   
        if not value or value.strip() == '':
            return None
        
        if value.strip().upper() in ('Y', 'N'):
            return None
        
        cleaned_value = value.replace(',', '').strip()
        
        if cleaned_value.startswith('(') and cleaned_value.endswith(')'):
            cleaned_value = '-' + cleaned_value[1:-1]
        
        try:
            return float(cleaned_value)
        except ValueError:
            logger.warning(f"Could not parse numeric value: {value}")
            return None

