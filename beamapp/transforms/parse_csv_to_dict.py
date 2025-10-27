import apache_beam as beam
import logging
from datetime import datetime
import csv
from io import StringIO

class ParseCsvToDict(beam.DoFn):
    # Improved CSV parsing function using csv.DictReader to make it dynamic
    def process(self, element):
        """Parses a CSV line into a dictionary with proper data types."""
        if not element:
            return  # Skip empty lines
        
        
        # Convert transaction_amount to float and changing date string to YYYY-MM-DD date object
        try:
            f = StringIO(element)
            reader = csv.DictReader(f, fieldnames=['date', 'origin', 'destination', 'transaction_amount'])
            row = next(reader)
            
            if not row['date'] or not row['transaction_amount']:
                logging.warning(f"Skipping row due to missing date or transaction_amount: {element}")
                return  # Skip rows with missing essential fields
            
            try:
                row['transaction_amount'] = float(row['transaction_amount'])
            except ValueError:
                logging.warning(f"Skipping row due to invalid transaction_amount: {element}")
                return  # Skip rows with invalid transaction_amount
            #Remove UTC and convert to date object
            timestamp = row['date'].replace(" UTC", "")
            try:
                row['date'] = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').date()
            except ValueError:
                logging.warning(f"Skipping row due to invalid date format: {element}")
                return  # Skip rows with invalid date format
            
            # Successfully parsed row, yield it
            logging.debug(f"Parsed row: {row}")  # Debugging line (optional, for development)
            yield row
            
        except (KeyError, TypeError, ValueError) as e:
            logging.error(f"Error parsing line: {element}, Error: {e}")
            return
        
        
        