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

    
    # pipeline
    try:
        logger.info("Extracting data from PDF...")
        # step 1 : Parsing pdf
        records = parse_enr_pdf(str(pdf_file))
        # Save step 1 snapshot
        save_stage_snapshot(records, "step1_raw_records.json")
        # step 2 : extracting location
        records = add_location_field(records)
        # Save step 2 snapshot
        save_stage_snapshot(records, "step2_records_with_loc.json")
        # Enrichment pipeline
        # Stage 3: DDG for all records 
        records = enrich_batch_with_duckduckgo(records, batch_size=15)
        # Save step 3 snapshot
        save_stage_snapshot(records, "step3_records_after_ddg.json")
        # Stage 4: LLM +Scoring for all records 
        records = enrich_batch_with_llm(records, batch_size=10)
        # Save step 4 snapshot (final)
        save_stage_snapshot(records, "step4_records_after_llm_scoring.json")

        enriched_records = records
        logger.info("Batch enrichment pipeline completed.")

        # Convert to DataFrame
        df = pd.DataFrame(enriched_records)
        
        logger.info("Successfully processed %d records", len(df))
        logger.info("Displaying a sample of the first 10 enriched rows below")
        
        # Display first 10 rows
        print(tabulate(df.head(10), headers="keys", tablefmt="psql"))
        
        # Save to CSV
        output_file = "enr_parsed_enriched.csv"
        df.to_csv(output_file, index=False)
        logger.info("Saved CSV: %s", output_file)
        
        # Save to JSON (list of dictionaries)
        json_output_file = "enr_parsed_enriched.json"
        with open(json_output_file, "w", encoding="utf-8") as f:
            json.dump(enriched_records, f, ensure_ascii=False, indent=2)
        logger.info("Saved JSON: %s", json_output_file)
        
        # Print summary statistics
        logger.info("Summary statistics (if columns available):")
        if 'revenue_m' in df.columns:
            avg_rev = f"${df['revenue_m'].mean():,.2f}M"
            max_rev = f"${df['revenue_m'].max():,.2f}M"
            min_rev = f"${df['revenue_m'].min():,.2f}M"
            logger.info("Average Revenue: %s", avg_rev)
            logger.info("Max Revenue: %s", max_rev)
            logger.info("Min Revenue: %s", min_rev)
        if 'rank_2025' in df.columns:
            logger.info("Total Firms in Ranking: %d", len(df))
        
    except Exception as e:
        logger.exception("Error during pipeline execution: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
