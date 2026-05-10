#!/usr/bin/env python3
"""
Ad Performance Aggregator — CLI Application

Processes a large CSV file of advertising performance data, aggregates
metrics by campaign_id, and outputs the top 10 campaigns by CTR and CPA.

Usage:
    python aggregator.py --input ad_data.csv --output results/
"""

import argparse
import logging
import os
import sys
import time
import tracemalloc

from core.processor import process_csv
from core.writer import write_top10_ctr, write_top10_cpa


def setup_logging():
    """Configure logging to output to console with timestamps."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Ad Performance Aggregator — Process ad data CSV and generate top campaign reports.",
    )
    parser.add_argument(
        "--input",
        type=str,
        default="ad_data.csv",
        help="Path to the input CSV file (default: ad_data.csv)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="results/",
        help="Directory to save result CSV files (default: results/)",
    )
    return parser.parse_args()


def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    args = parse_args()

    input_path = args.input
    output_dir = args.output

    # --- Validate input file ---
    if not os.path.isfile(input_path):
        logger.error(f"Input file not found: {input_path}")
        sys.exit(1)

    # --- Validate output directory ---
    try:
        os.makedirs(output_dir, exist_ok=True)
    except PermissionError:
        logger.error(f"Permission denied: cannot create output directory '{output_dir}'")
        sys.exit(1)

    # --- Start processing ---
    logger.info("=" * 60)
    logger.info("Ad Performance Aggregator")
    logger.info("=" * 60)
    logger.info(f"Input file : {input_path}")
    logger.info(f"Output dir : {output_dir}")

    file_size_mb = os.path.getsize(input_path) / (1024 * 1024)
    logger.info(f"File size  : {file_size_mb:.1f} MB")
    logger.info("-" * 60)

    # Start memory tracking
    tracemalloc.start()
    start_time = time.time()

    # Step 1: Process CSV
    logger.info("Step 1/3: Reading and aggregating CSV data...")
    try:
        results = process_csv(input_path)
    except ValueError as e:
        logger.error(f"CSV processing error: {e}")
        sys.exit(1)
    except PermissionError:
        logger.error(f"Permission denied: cannot read '{input_path}'")
        sys.exit(1)

    logger.info(f"  Found {len(results)} unique campaigns.")

    # Step 2: Write top 10 CTR
    logger.info("Step 2/3: Writing top 10 campaigns by CTR...")
    try:
        write_top10_ctr(results, output_dir)
    except PermissionError:
        logger.error(f"Permission denied: cannot write to '{output_dir}'")
        sys.exit(1)

    # Step 3: Write top 10 CPA
    logger.info("Step 3/3: Writing top 10 campaigns by lowest CPA...")
    try:
        write_top10_cpa(results, output_dir)
    except PermissionError:
        logger.error(f"Permission denied: cannot write to '{output_dir}'")
        sys.exit(1)

    # --- Report ---
    elapsed = time.time() - start_time
    current_mem, peak_mem = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    logger.info("-" * 60)
    logger.info(f"Processing time : {elapsed:.2f} seconds")
    logger.info(f"Peak memory     : {peak_mem / (1024 * 1024):.2f} MB")
    logger.info("=" * 60)
    logger.info("Done! Results saved to:")
    logger.info(f"  - {os.path.join(output_dir, 'top10_ctr.csv')}")
    logger.info(f"  - {os.path.join(output_dir, 'top10_cpa.csv')}")


if __name__ == "__main__":
    main()
