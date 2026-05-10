"""
Result writer for ad performance aggregation.

Sorts aggregated campaign data and writes the Top 10 results
to CSV files.
"""

import csv
import os
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

OUTPUT_COLUMNS = [
    "campaign_id",
    "total_impressions",
    "total_clicks",
    "total_spend",
    "total_conversions",
    "CTR",
    "CPA",
]


def write_top10_ctr(data: List[Dict], output_dir: str) -> str:
    """
    Write the top 10 campaigns with the highest CTR to a CSV file.

    Args:
        data: List of aggregated campaign dicts.
        output_dir: Directory to write the output file.

    Returns:
        Path to the written file.
    """
    # Sort by CTR descending
    sorted_data = sorted(data, key=lambda x: x["CTR"], reverse=True)
    top10 = sorted_data[:10]

    output_path = os.path.join(output_dir, "top10_ctr.csv")
    _write_csv(top10, output_path)
    logger.info(f"  Written: {output_path} ({len(top10)} campaigns)")
    return output_path


def write_top10_cpa(data: List[Dict], output_dir: str) -> str:
    """
    Write the top 10 campaigns with the lowest CPA to a CSV file.
    Excludes campaigns with 0 conversions (CPA is None).

    Args:
        data: List of aggregated campaign dicts.
        output_dir: Directory to write the output file.

    Returns:
        Path to the written file.
    """
    # Filter out campaigns with no conversions
    valid = [d for d in data if d["CPA"] is not None]

    # Sort by CPA ascending (lowest first)
    sorted_data = sorted(valid, key=lambda x: x["CPA"])
    top10 = sorted_data[:10]

    output_path = os.path.join(output_dir, "top10_cpa.csv")
    _write_csv(top10, output_path)
    logger.info(f"  Written: {output_path} ({len(top10)} campaigns)")
    return output_path


def _write_csv(rows: List[Dict], output_path: str) -> None:
    """Write a list of dicts to a CSV file with the standard output columns."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        for row in rows:
            # Format CPA as empty string if None (null)
            formatted_row = dict(row)
            if formatted_row["CPA"] is None:
                formatted_row["CPA"] = ""
            writer.writerow(formatted_row)
