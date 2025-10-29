"""
main.py
Purpose:
  Entry point for running the ENR PDF parser.
  Parses the PDF file from the input folder and outputs the results.
"""

import sys
from pathlib import Path

# Import the parse_enr_pdf function from demo.py
from pdf_parser import parse_enr_pdf
import pandas as pd
from tabulate import tabulate
import json
from uitility import add_location_field, save_stage_snapshot
from duckduckgo_enricher import enrich_batch_with_duckduckgo
from llm_enricher import enrich_batch_with_llm
from scoring import score_record
import logging
from pdf_cutter import cut_pdf

def main():
    """
    Main function to parse the ENR PDF file and display/save results.
    """
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logger = logging.getLogger("main")
    input_folder = Path(__file__).parent / "input" 
    pdf_file = input_folder / "sampe_to_test_50_records.pdf"
    if not pdf_file.exists():
        print(f"❌ Error: PDF file not found at {pdf_file}")
        print("Please ensure the PDF file exists in the input folder.")
        sys.exit(1)
    
    logger.info("Parsing PDF file: %s", pdf_file)

    try:
        logger.info("Extracting data from PDF...")
        # step 1 : Parsing pdf
        records = parse_enr_pdf(str(pdf_file))
    

    except Exception as e:
    logger.exception("Error during pipeline execution: %s", e)
    sys.exit(1)


if __name__ == "__main__":
    main()
