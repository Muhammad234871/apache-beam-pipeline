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
        
        row = self.parse_csv_line(element)
        if row is None:
            return  # Skip rows that failed to parse
        
        yield row
        
    def parse_csv_line(self, element):
        """Helper function to parse a CSV line into a dictionary."""
        f = StringIO(element)
        reader = csv.DictReader(f, fieldnames=['date', 'origin', 'destination', 'transaction_amount'])
        
         # Convert transaction_amount to float and changing date string to YYYY-MM-DD date object
        try:
            f = StringIO(element)
            reader = csv.DictReader(f, fieldnames=['date', 'origin', 'destination', 'transaction_amount'])
            row = next(reader)
            
            if not row['date'] or not row['transaction_amount']:
                logging.warning(f"Skipping row due to missing date or transaction_amount: {element}")
                return  # Skip rows with missing essential fields
            
            # Convert transaction_amount to float
            row['transaction_amount'] = self.float_convert(row['transaction_amount'])
            if row['transaction_amount'] is None:
                return  # Skip rows with invalid transaction_amount
            
            
            #Convert date string to date object
            row['date'] = self.date_convert(row['date'])
            if row['date'] is None:
                return  # Skip rows with invalid date format
            
            
            # Successfully parsed row, yield it
            logging.debug(f"Parsed row: {row}")  # Debugging line (optional, for development)
            return row
            
        except (KeyError, TypeError, ValueError) as e:
            logging.error(f"Error parsing line: {element}, Error: {e}")
            return None
        
        
    def float_convert(self, value):
        try:
            return float(value)
        
        except ValueError:
            logging.warning(f"Skipping row due to invalid transaction_amount: {value}")
            return  None# Skip rows with invalid transaction_amount
        
        
    def date_convert(self, date_str):
        #Remove UTC and convert to date object
           
        try:
            timestamp = date_str.replace(" UTC", "")
            return datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').date()
        except ValueError:
            logging.warning(f"Skipping row due to invalid date format: {date_str}")
            return None # Skip rows with invalid date format