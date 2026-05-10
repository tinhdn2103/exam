"""
Optimized CSV processor for ad performance data using Pandas.

Uses pandas with chunking to leverage C-optimized parsing and 
vectorized aggregation, significantly improving processing speed
for large files while maintaining low memory usage.
"""

import pandas as pd
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# Expected CSV columns
EXPECTED_COLUMNS = ["campaign_id", "date", "impressions", "clicks", "spend", "conversions"]
NUMERIC_COLUMNS = ["impressions", "clicks", "spend", "conversions"]


def process_csv(input_path: str, chunk_size: int = 100_000) -> List[Dict]:
    """
    Process a CSV file using pandas chunking and aggregate data by campaign_id.

    Args:
        input_path: Path to the input CSV file.
        chunk_size: Number of rows to process per chunk.

    Returns:
        A list of dicts, each containing aggregated metrics for one campaign.
    """
    aggregator = pd.DataFrame()
    row_count = 0
    skipped_count = 0

    try:
        # Use pandas read_csv with chunksize
        # To handle malformed rows (like 'bad_number' in an int column) without failing,
        # we read all as string/object first and then convert.
        reader = pd.read_csv(
            input_path, 
            chunksize=chunk_size, 
            usecols=EXPECTED_COLUMNS,
            dtype={'campaign_id': 'string', 'date': 'string'},
            engine='c'
        )

        for chunk in reader:
            initial_len = len(chunk)
            
            # Convert numeric columns, coersion creates NaN for malformed data
            for col in NUMERIC_COLUMNS:
                chunk[col] = pd.to_numeric(chunk[col], errors='coerce')
            
            # Filter out rows with NaN or negative values
            # (Matches the logic in the previous row-by-row implementation)
            valid_mask = chunk[NUMERIC_COLUMNS].notna().all(axis=1)
            valid_mask &= (chunk['campaign_id'].notna()) & (chunk['campaign_id'].str.strip() != "")
            
            # Check for negative values
            for col in NUMERIC_COLUMNS:
                valid_mask &= (chunk[col] >= 0)
            
            chunk = chunk[valid_mask]
            
            skipped_count += (initial_len - len(chunk))
            row_count += initial_len
            
            if len(chunk) > 0:
                # Aggregate within the chunk
                chunk_agg = chunk.groupby('campaign_id')[NUMERIC_COLUMNS].sum().reset_index()
                
                # Combine with global aggregator
                if aggregator.empty:
                    aggregator = chunk_agg
                else:
                    aggregator = pd.concat([aggregator, chunk_agg]).groupby('campaign_id')[NUMERIC_COLUMNS].sum().reset_index()

            if row_count % 1_000_000 == 0 or row_count == initial_len:
                 logger.info(f"  Processed {row_count:,} rows...")

    except ValueError as e:
        # Re-raise with expected message for tests if it's about missing columns
        if "Usecols do not match" in str(e):
             raise ValueError(f"CSV is missing required columns: {e}")
        raise
    except Exception as e:
        logger.error(f"Error during CSV processing: {e}")
        raise ValueError(f"Failed to process CSV: {e}")

    logger.info(f"  Total rows processed: {row_count:,}")
    if skipped_count > 0:
        logger.warning(f"  Skipped {skipped_count:,} malformed rows.")

    if aggregator.empty:
        return []

    # Compute derived metrics using vectorized operations
    aggregator['CTR'] = aggregator['clicks'] / aggregator['impressions']
    aggregator['CTR'] = aggregator['CTR'].fillna(0.0).round(4)
    
    # CPA = spend / conversions (null if conversions is 0)
    aggregator['CPA'] = aggregator['spend'] / aggregator['conversions']
    
    # Replace inf with NaN, round, then convert to None for the final list
    # inf happens if conversions is 0 and spend > 0
    # NaN happens if both are 0
    import numpy as np
    aggregator['CPA'] = aggregator['CPA'].replace([np.inf, -np.inf], np.nan)
    aggregator['CPA'] = aggregator['CPA'].round(2)

    # Rename columns to match requirements
    aggregator = aggregator.rename(columns={
        'impressions': 'total_impressions',
        'clicks': 'total_clicks',
        'spend': 'total_spend',
        'conversions': 'total_conversions'
    })

    # Convert back to list of dictionaries
    results = aggregator.to_dict(orient='records')
    
    # Ensure CPA is None (not NaN) for downstream compatibility (csv writer)
    for row in results:
        if pd.isna(row['CPA']):
            row['CPA'] = None
            
    return results
