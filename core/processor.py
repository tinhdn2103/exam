"""
Memory-efficient CSV processor for ad performance data using pure streaming.

This version avoids Pandas to minimize memory footprint, using Python's built-in 
csv module. It still utilizes multiprocessing for speed by splitting the file 
into byte-segments, but each worker process only maintains a small dictionary 
of aggregated metrics, ensuring memory usage stays low regardless of file size.
"""

import csv
import logging
import os
from typing import Dict, List, Tuple
from concurrent.futures import ProcessPoolExecutor

logger = logging.getLogger(__name__)

# Expected CSV columns (0: id, 1: date, 2: imp, 3: clk, 4: spend, 5: cnv)
EXPECTED_COLUMNS = ["campaign_id", "date", "impressions", "clicks", "spend", "conversions"]

def process_segment(input_path: str, start: int, end: int) -> Tuple[Dict, int, int]:
    """
    Process a segment of the CSV file using streaming.
    Returns (aggregated_dict, total_rows, skipped_rows).
    aggregated_dict format: {campaign_id: [imp, clk, spend, cnv]}
    """
    segment_agg = {}
    total_rows = 0
    skipped_count = 0
    
    try:
        with open(input_path, 'rb') as f:
            if start > 0:
                f.seek(start)
                # Skip the first partial line
                f.readline()
            
            # Use a generator to read lines until the end of the segment
            def line_generator():
                while f.tell() < end:
                    line = f.readline()
                    if not line:
                        break
                    yield line.decode('utf-8')

            # Use csv.reader on the line generator
            reader = csv.reader(line_generator())
            
            for row in reader:
                if not row or len(row) < 6:
                    skipped_count += 1
                    continue
                
                try:
                    # campaign_id, date, impressions, clicks, spend, conversions
                    cid = row[0].strip()
                    if not cid:
                        skipped_count += 1
                        continue
                        
                    # Fast conversion
                    imp = int(row[2])
                    clk = int(row[3])
                    spd = float(row[4])
                    cnv = int(row[5])
                    
                    if imp < 0 or clk < 0 or spd < 0 or cnv < 0:
                        skipped_count += 1
                        continue
                        
                    if cid not in segment_agg:
                        segment_agg[cid] = [0, 0, 0.0, 0]
                    
                    metrics = segment_agg[cid]
                    metrics[0] += imp
                    metrics[1] += clk
                    metrics[2] += spd
                    metrics[3] += cnv
                    total_rows += 1
                    
                except (ValueError, IndexError):
                    skipped_count += 1
                    continue
                    
    except Exception as e:
        logger.debug(f"Segment [{start}-{end}] error: {e}")
        
    return segment_agg, total_rows, skipped_count


def process_csv(input_path: str) -> List[Dict]:
    """
    Process a CSV file using Multiprocessing and streaming aggregation.
    """
    # Check for required columns in header
    with open(input_path, 'r', encoding='utf-8') as f:
        header = f.readline().strip().split(',')
        missing = [col for col in EXPECTED_COLUMNS if col not in header]
        if missing:
            raise ValueError(f"CSV is missing required columns: {', '.join(missing)}")

    file_size = os.path.getsize(input_path)
    # Heuristic: if file is small, process sequentially
    if file_size < 5 * 1024 * 1024:
        return _process_csv_sequential(input_path)

    num_workers = min(os.cpu_count() or 4, 8)
    segment_size = file_size // num_workers
    
    segments = []
    for i in range(num_workers):
        start = i * segment_size
        end = (i + 1) * segment_size if i < num_workers - 1 else file_size
        segments.append((input_path, start, end))

    logger.info(f"Starting multiprocessing with {num_workers} workers (Streaming mode)...")
    
    global_agg = {}
    total_rows = 0
    total_skipped = 0

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(process_segment, *seg) for seg in segments]
        
        for future in futures:
            seg_agg, seg_rows, seg_skipped = future.result()
            total_rows += seg_rows
            total_skipped += seg_skipped
            
            # Merge worker results into global aggregation
            for cid, metrics in seg_agg.items():
                if cid not in global_agg:
                    global_agg[cid] = metrics
                else:
                    target = global_agg[cid]
                    target[0] += metrics[0]
                    target[1] += metrics[1]
                    target[2] += metrics[2]
                    target[3] += metrics[3]

    return _finalize_results(global_agg, total_rows, total_skipped)


def _process_csv_sequential(input_path: str) -> List[Dict]:
    """Fallback sequential processing for small files."""
    agg = {}
    row_count = 0
    skipped_count = 0

    try:
        with open(input_path, 'r', encoding='utf-8', newline='') as f:
            # Check for header
            header = f.readline()
            if not header.startswith('campaign_id'):
                 # Not a header, go back to start
                 f.seek(0)
            
            reader = csv.reader(f)
            for row in reader:
                if not row or len(row) < 6:
                    skipped_count += 1
                    continue
                try:
                    cid = row[0].strip()
                    if not cid:
                        skipped_count += 1
                        continue
                    imp, clk, spd, cnv = int(row[2]), int(row[3]), float(row[4]), int(row[5])
                    if imp < 0 or clk < 0 or spd < 0 or cnv < 0:
                        skipped_count += 1
                        continue
                        
                    if cid not in agg:
                        agg[cid] = [0, 0, 0.0, 0]
                    m = agg[cid]
                    m[0] += imp; m[1] += clk; m[2] += spd; m[3] += cnv
                    row_count += 1
                except (ValueError, IndexError):
                    skipped_count += 1
    except Exception as e:
        raise ValueError(f"Failed to process CSV: {e}")

    return _finalize_results(agg, row_count, skipped_count)


def _finalize_results(aggregator_dict: Dict, total_rows: int, skipped_count: int) -> List[Dict]:
    """Perform final calculations and format results."""
    logger.info(f"  Total rows processed: {total_rows:,}")
    if skipped_count > 0:
        logger.warning(f"  Skipped {skipped_count:,} malformed rows.")

    results = []
    for cid, m in aggregator_dict.items():
        imp, clk, spend, cnv = m
        
        ctr = clk / imp if imp > 0 else 0.0
        cpa = spend / cnv if cnv > 0 else None
        
        results.append({
            'campaign_id': cid,
            'total_impressions': imp,
            'total_clicks': clk,
            'total_spend': round(spend, 2),
            'total_conversions': cnv,
            'CTR': round(ctr, 4),
            'CPA': round(cpa, 2) if cpa is not None else None
        })
            
    return results
