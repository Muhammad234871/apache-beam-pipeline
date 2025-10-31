import json
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

# import the ProcessTransactions composite transform
from beamapp.transforms.process_transactions import ProcessTransactions

# Common test data setup
def input_lines():
     # Input CSV lines including a header, a below-threshold amount, and mixed years
    return [
        "timestamp,origin,destination,transaction_amount",
        "2009-12-31 23:59:59 UTC,walletA,walletB,99.00",      # year < 2010 → filtered out
        "2010-01-01 00:00:00 UTC,walletX,walletY,10.00",       # amount <= 20 → filtered out
        "2010-01-01 01:00:00 UTC,walletX,walletY,25.00",       # kept: 2010-01-01 → 25.00
        "2011-01-01 04:00:00 UTC,walletA,walletB,30.00",       # kept: 2011-01-01 → 30.00
        "2011-01-01 05:00:00 UTC,walletC,walletD,5.00",        # amount <= 20 → filtered out
        "2011-01-02 06:00:00 UTC,walletE,walletF,100.00",      # kept: 2011-01-02 → 100.00
        "2011-01-02 07:00:00 UTC,walletG,walletH,35.00",       # kept: 2011-01-02 → +35.00
        "2017-03-18 14:09:16 UTC,walletK,walletL,2102.22",     # kept: 2017-03-18 → 2102.22
        "2018-02-27 16:04:11 UTC,walletM,walletN,129.12",      # kept: 2018-02-27 → 129.12
        # malformed lines will be safely skipped by the parser:
        "badline",
        "2018-02-27 16:04:11 UTC,walletM,walletN,not_a_number",
    ]

# Expected output after processing
def expected_output():
    return [
        json.dumps({"date": "2010-01-01", "total_transaction_amount": 25.00}),
        json.dumps({"date": "2011-01-01", "total_transaction_amount": 30.00}),
        json.dumps({"date": "2017-03-18", "total_transaction_amount": 2102.22}),
        json.dumps({"date": "2011-01-02", "total_transaction_amount": 135.00}),
        json.dumps({"date": "2018-02-27", "total_transaction_amount": 129.12}),
    ]


# Test case for ProcessTransactions transform with typical input and expected output
def test_process_transactions():
    
    # Use the TestPipeline manager
    with TestPipeline() as p:
        output = (
            p
            | "Create Input" >> beam.Create(input_lines()) # Simulate input lines for test as a PCollection
            | "Process Transactions" >> ProcessTransactions() #Composite transform
            
        )

        # Assertion must happen inside the pipeline context
        assert_that(output, equal_to(expected_output()))

#test case for empty input
def test_process_transactions_empty_input():
    with TestPipeline() as p:
        output = (
            p
            | "Create Input" >> beam.Create([])     # no input lines
            | "Process Transactions" >> ProcessTransactions()
        )
        # Checking if both outputs are equal 
        assert_that(output, equal_to([]))
