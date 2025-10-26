import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from .pardos.process_transactions import ProcessTransactions


def run(argv=None):
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--runner", default="DirectRunner")
    
    args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args, save_main_session=True)
    options.view_as(StandardOptions).runner = args.runner

    data= "gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv"


    with beam.Pipeline(options=options) as p:
        # Process the pipeline
        (
            p
            | "Create Input" >> beam.io.ReadFromText(data, skip_header_lines=1)  # Reading data from GCS
            | "Process Transactions" >> ProcessTransactions()  # Using the composite transform
            | "Write Gzip Output" >> beam.io.WriteToText(
                "output/result",
                file_name_suffix=".json1.gz",
                compression_type=beam.io.filesystem.CompressionTypes.GZIP,
                shard_name_template=""
                )
        )

