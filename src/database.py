"""
Module for database operations.
"""

import logging
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from typing import List, Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)

class Database:
    """Class for database operations."""
    
    def __init__(self, host: str, port: str, database: str, user: str, password: str):
        """
        Initialize the database connection.
        
        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
        """
        self.connection_params = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }
        self.conn = None
        self.connect()
    
    def connect(self):
        """Establish a connection to the database."""
        try:
            self.conn = psycopg2.connect(**self.connection_params)
            logger.info("Connected to the database")
        except psycopg2.Error as e:
            logger.error(f"Error connecting to the database: {str(e)}")
            raise
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def initialize_database(self):
        """Create the necessary tables if they don't exist."""
        try:
            with self.conn.cursor() as cursor:
                # Create the gas_shipments table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS gas_shipments (
                        id SERIAL PRIMARY KEY,
                        loc TEXT NOT NULL,
                        loc_zone TEXT,
                        loc_name TEXT,
                        loc_purpose TEXT,
                        measure_basis TEXT,
                        oper_capacity NUMERIC,
                        design_capacity NUMERIC,
                        scheduled_qty NUMERIC,
                        operationally_available NUMERIC,
                        total_scheduled NUMERIC,
                        gas_day DATE NOT NULL,
                        cycle INTEGER NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(loc, gas_day, cycle)
                    );
                    
                    -- Create indexes for better query performance
                    CREATE INDEX IF NOT EXISTS idx_gas_shipments_gas_day ON gas_shipments(gas_day);
                    CREATE INDEX IF NOT EXISTS idx_gas_shipments_loc ON gas_shipments(loc);
                """)
                
                self.conn.commit()
                logger.info("Database initialized successfully")
        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error initializing database: {str(e)}")
            raise
    
    def deduplicate_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Deduplicate records based on the unique constraint (loc, gas_day, cycle).
        
        Args:
            records: List of record dictionaries to deduplicate
            
        Returns:
            Deduplicated list of records
        """
        if not records:
            return []
        
        # Use a dictionary to deduplicate records
        unique_records = {}
        for record in records:
            # Create a unique key based on the UNIQUE constraint columns
            key = (record['loc'], record['gas_day'], record['cycle'])
            
            # Keep only the last record for each unique key
            unique_records[key] = record
        
        # Convert back to a list
        deduplicated = list(unique_records.values())
        
        logger.info(f"Deduplicated {len(records)} records to {len(deduplicated)} unique records")
        
        return deduplicated
    
    def insert_records(self, records: List[Dict[str, Any]]) -> int:
        """
        Insert records into the gas_shipments table.
        
        Args:
            records: List of record dictionaries to insert
            
        Returns:
            Number of records inserted
        """
        if not records:
            logger.warning("No records to insert")
            return 0
        
        try:
            # Deduplicate records before insertion
            deduplicated_records = self.deduplicate_records(records)
            
            if not deduplicated_records:
                logger.warning("No unique records to insert after deduplication")
                return 0
            
            with self.conn.cursor() as cursor:
                # Prepare the columns and values
                columns = list(deduplicated_records[0].keys())
                values = [[record.get(column) for column in columns] for record in deduplicated_records]
                
                # Build the SQL query
                query = sql.SQL("""
                    INSERT INTO gas_shipments ({})
                    VALUES %s
                    ON CONFLICT (loc, gas_day, cycle) DO UPDATE
                    SET
                        loc_zone = EXCLUDED.loc_zone,
                        loc_name = EXCLUDED.loc_name,
                        loc_purpose = EXCLUDED.loc_purpose,
                        measure_basis = EXCLUDED.measure_basis,
                        oper_capacity = EXCLUDED.oper_capacity,
                        design_capacity = EXCLUDED.design_capacity,
                        scheduled_qty = EXCLUDED.scheduled_qty,
                        operationally_available = EXCLUDED.operationally_available,
                        total_scheduled = EXCLUDED.total_scheduled,
                        created_at = CURRENT_TIMESTAMP
                    RETURNING id
                """).format(
                    sql.SQL(', ').join(map(sql.Identifier, columns))
                )
                
                # Execute the query
                result = execute_values(cursor, query, values, fetch=True)
                
                self.conn.commit()
                inserted_count = len(result)
                logger.info(f"Inserted {inserted_count} records into the database")
                return inserted_count
                
        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error inserting records: {str(e)}")
            raise
    
    def insert_records_in_batches(self, records: List[Dict[str, Any]], batch_size: int = 1000) -> int:
        """
        Insert records into the gas_shipments table in batches to avoid conflicts.
        
        Args:
            records: List of record dictionaries to insert
            batch_size: Number of records to insert in each batch
            
        Returns:
            Total number of records inserted
        """
        if not records:
            logger.warning("No records to insert")
            return 0
        
        # Deduplicate all records first
        deduplicated_records = self.deduplicate_records(records)
        
        if not deduplicated_records:
            logger.warning("No unique records to insert after deduplication")
            return 0
        
        total_inserted = 0
        
        try:
            # Process records in batches
            for i in range(0, len(deduplicated_records), batch_size):
                batch = deduplicated_records[i:i + batch_size]
                
                with self.conn.cursor() as cursor:
                    # Prepare the columns and values for this batch
                    columns = list(batch[0].keys())
                    values = [[record.get(column) for column in columns] for record in batch]
                    
                    # Build the SQL query
                    query = sql.SQL("""
                        INSERT INTO gas_shipments ({})
                        VALUES %s
                        ON CONFLICT (loc, gas_day, cycle) DO UPDATE
                        SET
                            loc_zone = EXCLUDED.loc_zone,
                            loc_name = EXCLUDED.loc_name,
                            loc_purpose = EXCLUDED.loc_purpose,
                            measure_basis = EXCLUDED.measure_basis,
                            oper_capacity = EXCLUDED.oper_capacity,
                            design_capacity = EXCLUDED.design_capacity,
                            scheduled_qty = EXCLUDED.scheduled_qty,
                            operationally_available = EXCLUDED.operationally_available,
                            total_scheduled = EXCLUDED.total_scheduled,
                            created_at = CURRENT_TIMESTAMP
                        RETURNING id
                    """).format(
                        sql.SQL(', ').join(map(sql.Identifier, columns))
                    )
                    
                    # Execute the query for this batch
                    result = execute_values(cursor, query, values, fetch=True)
                    
                    self.conn.commit()
                    batch_inserted = len(result)
                    total_inserted += batch_inserted
                    logger.info(f"Inserted batch of {batch_inserted} records (total: {total_inserted})")
            
            return total_inserted
                
        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error inserting records: {str(e)}")
            raise

