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
import threading
import psutil

from core.processor import process_csv
from core.writer import write_top10_ctr, write_top10_cpa


class MemoryMonitor(threading.Thread):
    """
    Monitor memory usage of the current process and all its children.
    This is necessary because ProcessPoolExecutor spawns separate processes
    whose memory is not tracked by tracemalloc in the main process.
    """
    def __init__(self, interval=0.1):
        super().__init__(daemon=True)
        self.interval = interval
        self.peak_memory = 0
        self.running = False
        self.root_process = psutil.Process(os.getpid())

    def run(self):
        self.running = True
        while self.running:
            try:
                # Sum RSS of main process and all children
                total_mem = self.root_process.memory_info().rss
                for child in self.root_process.children(recursive=True):
                    try:
                        total_mem += child.memory_info().rss
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue
                
                if total_mem > self.peak_memory:
                    self.peak_memory = total_mem
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
            time.sleep(self.interval)

    def stop(self):
        self.running = False


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
    mem_monitor = MemoryMonitor()
    mem_monitor.start()
    
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
    mem_monitor.stop()
    mem_monitor.join()
    
    current_mem, peak_mem_traced = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    peak_mem_rss = mem_monitor.peak_memory

    logger.info("-" * 60)
    logger.info(f"Processing time : {elapsed:.2f} seconds")
    logger.info(f"Peak memory (RSS): {peak_mem_rss / (1024 * 1024):.2f} MB")
    logger.info(f"Python-only peak: {peak_mem_traced / (1024 * 1024):.2f} MB")
    logger.info("=" * 60)
    logger.info("Done! Results saved to:")
    logger.info(f"  - {os.path.join(output_dir, 'top10_ctr.csv')}")
    logger.info(f"  - {os.path.join(output_dir, 'top10_cpa.csv')}")


if __name__ == "__main__":
    main()
