import argparse
import logging
from typing import Tuple
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# Import custom transforms and constants
from ..transforms.process_transactions import ProcessTransactions
from ..constants import INPUT_FILE_PATH, OUTPUT_FILE_PATH
from beamapp.utils.helpers import resolve_for_local


# parsing arguments 
def args_parser(argv=None):
    parser = argparse.ArgumentParser()
    
    # define arguments
    parser.add_argument("--runner", default="DirectRunner")
     # Parse known and unknown arguments
    args, pipeline_args = parser.parse_known_args(argv)
    return args, pipeline_args

# Create and configure pipeline options
def create_pipeline_options(pipeline_args, runner):
    """Creates and configures pipeline options."""
    options = PipelineOptions(pipeline_args, save_main_session=True)
    options.view_as(StandardOptions).runner = runner
    return options

# Resolve input/output paths based on runner type
def resolve_io_paths(runner: str) -> Tuple[str, str]:
    """
    Resolve input/output paths for local vs cloud execution.
    For local, allow helper to map relative fixtures; for cloud, use constants verbatim.
    """
    if runner == "DirectRunner":
        input_file = resolve_for_local(INPUT_FILE_PATH)
        output_prefix = resolve_for_local(OUTPUT_FILE_PATH)
    else:
        input_file = INPUT_FILE_PATH
        output_prefix = OUTPUT_FILE_PATH
    return input_file, output_prefix

# Main function to run the Beam pipeline
def run(argv=None):
    logging.basicConfig(level=logging.INFO)
    # Parse arguments
    args, pipeline_args = args_parser(argv)
    
    # Create and configure pipeline options
    options = create_pipeline_options(pipeline_args, args.runner)
        

    # Resolve input and output paths
    input_file, output_prefix = resolve_io_paths(args.runner)
    logging.info("Starting pipeline with runner=%s, input=%s, output_prefix=%s", args.runner, input_file, output_prefix)
    
    try:
        
        with beam.Pipeline(options=options) as p:
            # Process the pipeline
            (
                p
                | "Create Input" >> beam.io.ReadFromText(input_file, coder=beam.coders.StrUtf8Coder())  # Reading data from GCS
                | "Process Transactions" >> ProcessTransactions()  # Using the composite transform
                | "Write Gzip Output" >> beam.io.WriteToText(
                    output_prefix,
                    file_name_suffix=".json1.gz",
                    compression_type=beam.io.filesystem.CompressionTypes.GZIP,
                    shard_name_template=""
                    )
            )
               
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise
    
    logging.info("Pipeline completed successfully.")
