import logging
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from typing import List, Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)

class Database:
    
    def __init__(self, host, port, database, user, password):
       
        # Save connection parameters
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.conn = None
        
        self.connect()
    
    def connect(self):
        try:
            # Try to connect to the database
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            logger.info("Connected to the database")
        except psycopg2.OperationalError as e:
            if "database" in str(e) and "does not exist" in str(e):
                logger.warning(f"Database '{self.database}' does not exist. Attempting to create it...")
                
                try:
                    # Connect to the default 'postgres' database to create our database
                    temp_conn = psycopg2.connect(
                        host=self.host,
                        port=self.port,
                        database="postgres",  
                        user=self.user,
                        password=self.password
                    )
                    temp_conn.autocommit = True  
                    
                    with temp_conn.cursor() as cursor:
                        cursor.execute(f"CREATE DATABASE {self.database}")
                    
                    temp_conn.close()
                    
                    logger.info(f"Successfully created database '{self.database}'")
                    
                    # Now connect to the newly created database
                    self.conn = psycopg2.connect(
                        host=self.host,
                        port=self.port,
                        database=self.database,
                        user=self.user,
                        password=self.password
                    )
                    logger.info("Connected to the newly created database")
                    
                except Exception as create_error:
                    logger.error(f"Failed to create database: {str(create_error)}")
                    raise
            else:
                logger.error(f"Error connecting to the database: {str(e)}")
                raise
        except Exception as e:
            logger.error(f"Error connecting to the database: {str(e)}")
            raise
    
    def close(self):
        # Check if connection exists
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def initialize_database(self):
        try:
            cursor = self.conn.cursor()
            
            # SQL to create the table
            create_table_sql = """
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
            """
            
            cursor.execute(create_table_sql)
            
            # Create indexes for better performance
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_gas_shipments_gas_day ON gas_shipments(gas_day);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_gas_shipments_loc ON gas_shipments(loc);")
            
            self.conn.commit()
            
            cursor.close()
            
            logger.info("Database initialized successfully")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error initializing database: {str(e)}")
            raise
    
    def remove_duplicates(self, records):
        if not records:
            return []
        
        unique_records = {}
        
        # Loop through all records
        for record in records:
            
            key = (record['loc'], record['gas_day'], record['cycle'])     
            unique_records[key] = record
        
        # Convert the dictionary values back to a list
        deduplicated = list(unique_records.values())
        
        logger.info(f"Found {len(records) - len(deduplicated)} duplicate records")
        
        return deduplicated
    
    def insert_records(self, records):
       
        if not records:
            logger.warning("No records to insert")
            return 0
        
        try:
            # Remove duplicates before inserting
            unique_records = self.remove_duplicates(records)
            
            if not unique_records:
                logger.warning("No unique records to insert")
                return 0
            
            cursor = self.conn.cursor()
            
            columns = list(unique_records[0].keys())
            
            all_values = []
            for record in unique_records:
                record_values = []
                for column in columns:
                    record_values.append(record.get(column))
                all_values.append(record_values)
            
            # Build the SQL query
            column_names = ", ".join([f'"{col}"' for col in columns])
            placeholders = ", ".join(["%s"] * len(columns))
            
           
            query = f"""
                INSERT INTO gas_shipments ({column_names})
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
            """
            
            result = execute_values(cursor, query, all_values, fetch=True)
            
            self.conn.commit()
            
            cursor.close()
            
            # Return the number of inserted records
            inserted_count = len(result)
            logger.info(f"Inserted {inserted_count} records into the database")
            return inserted_count
                
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error inserting records: {str(e)}")
            raise
    
    def insert_in_small_groups(self, records, group_size=100):
        """
        Insert records into the database in small groups to avoid errors.
        """
        # Check if there are any records
        if not records:
            logger.warning("No records to insert")
            return 0
        
        # Remove duplicates first
        unique_records = self.remove_duplicates(records)
        
        if not unique_records:
            logger.warning("No unique records to insert")
            return 0
        
        # Keep track of how many records were inserted
        total_inserted = 0
        
        try:
            i = 0
            while i < len(unique_records):
                # Get a group of records
                group = unique_records[i:i+group_size]
                
                cursor = self.conn.cursor()
                
                # Get the column names from the first record
                columns = list(group[0].keys())
                
                # Create a list of values for each record
                group_values = []
                for record in group:
                    record_values = []
                    for column in columns:
                        record_values.append(record.get(column))
                    group_values.append(record_values)
                
                column_names = ", ".join([f'"{col}"' for col in columns])
                
                # Insert the group
                query = f"""
                    INSERT INTO gas_shipments ({column_names})
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
                """
                
                result = execute_values(cursor, query, group_values, fetch=True)
                
                self.conn.commit()
                
                cursor.close()
                
                # Update the total count
                group_inserted = len(result)
                total_inserted += group_inserted
                logger.info(f"Inserted group of {group_inserted} records (total: {total_inserted})")
                
                i += group_size
            
            return total_inserted
                
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error inserting records: {str(e)}")
            raise

