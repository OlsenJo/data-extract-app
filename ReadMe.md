# Natural gas Shipment Data Pipeline

This project downloads a CSV file with data about natural gas shipments from Energy Transfer's website validates the data and storesit in a database

## Features Overview

The pipeline performs the following task : 
1. Downloads CSV files from Energy Transfer's website for the last three days
2. Parses and validates all the data in the CSV files
3. Insets the data into a database

## Requirements

- Python 3.8 or higher
- PostgreSQL database
- Python packages in requirements.txt
  
## Installation

1. Clone the repository
```bash
https://github.com/OlsenJo/data-extract-app.git
cd data-extract-app