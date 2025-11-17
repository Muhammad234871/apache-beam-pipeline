import apache_beam as beam
import logging
from datetime import datetime
import csv
from io import StringIO

class ParseCsvToDict(beam.DoFn):

    def __init__(self): #implemented after interview
        self.header = None   # safe for Beam workers
        
        
    # Improved CSV parsing function using csv.DictReader to make it dynamic
    def process(self, element):
        """Parses a CSV line into a dictionary with proper data types."""
        line = element.strip()
        if not line:
            return
        
        # Detect and store header dynamically
        if self.header is None:
            self.header = next(csv.reader([line]))
            return  # do not emit the header row
        try:
            
            # Parse data row using stored header
            reader = csv.DictReader([line], fieldnames=self.header)
            row = next(reader)
        except Exception(csv.Error, StopIteration) as e:
            logging.error(f"Error parsing CSV line: {line}, Error: {e}")
            return

        # Clean + convert data
        parsed = self.clear_row(row)
        if parsed:
            yield parsed
    
    # Helper function to parse a CSV line into a dictionary
    def clear_row(self, row_line):
        """Helper function to parse a CSV line into a dictionary."""
        
         # Convert transaction_amount to float and changing date string to YYYY-MM-DD date object
        try:
            
            
            # Convert timestamp
            ts = self.date_convert(row_line["timestamp"])
            if ts is None:
                return None

            # Convert amount
            amt = self.float_convert(row_line["transaction_amount"])
            if amt is None:
                return None
            
            
            
            return {
                "timestamp": ts,
                "origin": row_line["origin"],
                "destination": row_line["destination"],
                "transaction_amount": amt,
            }
            
        except KeyError as e:
            logging.error(f"Missing field {e} in row: {row_line}")
            return None
        
    # Convert transaction_amount to float 
    def float_convert(self, value):
        try:
            return float(value)
        
        except ValueError:
            logging.warning(f"Skipping row due to invalid transaction_amount: {value}")
            return  None# Skip rows with invalid transaction_amount
        
    
    # Convert date string to date object    
    def date_convert(self, date_str):
        #Remove UTC and convert to date object
           
        try:
            timestamp = date_str.replace(" UTC", "")
            # Convert to date object
            return datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').date()
        except ValueError:
            logging.warning(f"Skipping row due to invalid date format: {date_str}")
            return None # Skip rows with invalid date format