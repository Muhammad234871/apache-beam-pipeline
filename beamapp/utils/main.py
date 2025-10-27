import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from ..transforms.process_transactions import ProcessTransactions
from ..constants import INPUT_FILE_PATH, OUTPUT_FILE_PATH


def args_parser(argv=None):
    parser = argparse.ArgumentParser()
    
    # define arguments
    parser.add_argument("--runner", default="DirectRunner")
    parser.add_argument("--input_file", default=INPUT_FILE_PATH, help="Path to the input file")
    parser.add_argument("--output_file", default=OUTPUT_FILE_PATH, help="Path to the output file")
    
     # Parse known and unknown arguments
    args, pipeline_args = parser.parse_known_args(argv)
    return args, pipeline_args

def create_pipeline_options(pipeline_args, runner):
    """Creates and configures pipeline options."""
    options = PipelineOptions(pipeline_args, save_main_session=True)
    options.view_as(StandardOptions).runner = runner
    return options


def run(argv=None):
    # Get argument parser
    # Parse arguments
    args, pipeline_args = args_parser(argv)
    
    # Create and configure pipeline options
    options = create_pipeline_options(pipeline_args, args.runner)

    try:
        
        with beam.Pipeline(options=options) as p:
            # Process the pipeline
            (
                p
                | "Create Input" >> beam.io.ReadFromText(INPUT_FILE_PATH, skip_header_lines=1, coder=beam.coders.StrUtf8Coder())  # Reading data from GCS
                | "Process Transactions" >> ProcessTransactions()  # Using the composite transform
                | "Write Gzip Output" >> beam.io.WriteToText(
                    OUTPUT_FILE_PATH,
                    file_name_suffix=".json1.gz",
                    compression_type=beam.io.filesystem.CompressionTypes.GZIP,
                    shard_name_template=""
                    )
            )
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise
    
    
    logging.info("Pipeline completed successfully.")
