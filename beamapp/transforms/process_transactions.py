import apache_beam as beam
from .extract_tranasction import ExtractTransaction
from .parse_csv_to_dict import ParseCsvToDict
import json


class ProcessTransactions(beam.PTransform):
    """Composite Transform to parse, filter, and aggregate transactions."""
    def expand(self, pcoll):
        return (
            pcoll
            | "Parse CSV" >> beam.ParDo(ParseCsvToDict())  # Parse CSV lines into dictionaries
            | "Extract Transaction" >> beam.ParDo(ExtractTransaction())  # Apply transformation (with the Help of ExtractTransaction class which is part of pardos module)
            | "Key Pairs" >> beam.Map(lambda x: (x['date'], x['transaction_amount']))  # Key by date and pass amount
            | "Sum Transactions by Date" >> beam.CombinePerKey(sum)  # Sum transactions per date
            | "As dict" >> beam.Map(lambda x: {'date': x[0], 'total_transaction_amount': x[1]})  # Convert back to dict
            | "To Json" >> beam.Map(json.dumps)  # Convert dict to string for output
        )
