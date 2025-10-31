import apache_beam as beam
import logging

class ExtractTransaction(beam.DoFn):
    def process(self, element):
        """Filters out transactions with a value less than 20. && Date greater than 2010"""
        
        # Skip None elements
        if not element or not isinstance(element, dict):
             # Debugging line
            return # Skip None elements

        try:
            # Convert date string to datetime object for comparison
            if 'transaction_amount' in element and 'date' in element:
                transaction_amount = float(element['transaction_amount'])
                date = element['date']         
                if transaction_amount > 20 and int(date.year) >= 2010:
                    element['date'] = date.isoformat()  # Convert date back to string for output
                    yield element
        except (KeyError, TypeError, ValueError) as e:
            logging.error(f"Error processing element: {element}, Error: {e}")
            return