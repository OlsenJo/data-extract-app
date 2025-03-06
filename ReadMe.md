# Natural Gas Shipment Data Pipeline

This project downloads CSV data about natural gas shipments from Energy Transfer's website, validates the data, and stores it in a PostgreSQL database.

## Overview

The pipeline performs the following tasks:
1. Downloads CSV files from Energy Transfer's website for the last three days
2. Stores the CSV files in a temporary folder
3. Parses and validates the data in the CSV files
4. Displays a summary of the data and asks for user confirmation
5. Inserts the data into a PostgreSQL database if confirmed

## Requirements

- Python 3.8 or higher
- PostgreSQL database
- Python packages (see src/requirements.txt)

## Installation

1. Clone the repository:
\`\`\`bash
git clone https://github.com/yourusername/gas-shipment-pipeline.git
cd gas-shipment-pipeline
\`\`\`

2. Install the required Python packages:
\`\`\`bash
pip install -r requirements.txt
\`\`\`

3. Configure PostgreSQL (see the PostgreSQL Configuration section below)

4. Create a `.env` file with your database configuration:
\`\`\`
DB_HOST=localhost
DB_PORT=5432
DB_NAME=gas_shipments
DB_USER=postgres
DB_PASSWORD=postgres
\`\`\`

## Usage

Run the pipeline with the following command:

\`\`\`bash
python main.py
\`\`\`

This will:
1. Download CSV files for the last three days and store them in the temporary folder
2. Parse and validate the data
3. Display a summary of the data to be inserted
4. Ask for your confirmation before proceeding
5. Insert the data into the PostgreSQL database if confirmed



## Configuration Options

You can configure the following options in the `.env` file:

- `DB_HOST`: Database host (default: localhost)
- `DB_PORT`: Database port (default: 5432)
- `DB_NAME`: Database name (default: gas_shipments)
- `DB_USER`: Database username (default: postgres)
- `DB_PASSWORD`: Database password (default: postgres)
- `TEMP_FOLDER`: Folder to store temporary CSV files (default: temp_csv)
- `KEEP_TEMP_FILES`: Whether to keep temporary CSV files after processing (default: True)

## CSV Data Format

The CSV files contain the following columns:
- `Loc`: Location identifier
- `Loc Zone`: Location zone
- `Loc Name`: Location name
- `Loc Purpose`: Location purpose
- `Meas Basis Desc`: Measurement basis description
- `Oper Capacity`: Operational capacity
- `Design Capacity`: Design capacity
- `Scheduled Qty`: Scheduled quantity
- `Operationally Available`: Operationally available
- `Total Scheduled`: Total scheduled


## Logging

The application logs information to both the console and a file called `pipeline.log`. You can check this file for detailed information about the pipeline's execution.

## Error Handling

The pipeline includes error handling for:
- Failed downloads
- Invalid CSV data
- Database connection issues
- Database insertion errors

If an error occurs during processing, the pipeline will log the error and continue processing other dates and cycles.

