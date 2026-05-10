"""
Unit tests for the Ad Performance Aggregator.

Tests cover:
- Normal aggregation across multiple campaigns and dates
- Empty CSV (header only)
- Campaigns with 0 conversions (CPA should be None)
- Campaigns with 0 impressions (CTR should be 0)
- Malformed rows are skipped gracefully
- Top 10 CTR and CPA sorting and output correctness
"""

import csv
import os
import tempfile

import pytest

from core.processor import process_csv
from core.writer import write_top10_ctr, write_top10_cpa


HEADER = "campaign_id,date,impressions,clicks,spend,conversions\n"


def _create_csv(tmp_path, filename, lines):
    """Helper to create a test CSV file."""
    filepath = os.path.join(str(tmp_path), filename)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        f.write(HEADER)
        for line in lines:
            f.write(line + "\n")
    return filepath


class TestProcessCSV:
    """Tests for core.processor.process_csv"""

    def test_normal_aggregation(self, tmp_path):
        """Multiple rows for the same campaign should be aggregated correctly."""
        filepath = _create_csv(tmp_path, "test.csv", [
            "CMP001,2025-01-01,12000,300,45.50,12",
            "CMP001,2025-01-02,14000,340,48.20,15",
            "CMP002,2025-01-01,8000,120,28.00,4",
        ])
        results = process_csv(filepath)

        assert len(results) == 2

        cmp001 = next(r for r in results if r["campaign_id"] == "CMP001")
        assert cmp001["total_impressions"] == 26000
        assert cmp001["total_clicks"] == 640
        assert cmp001["total_spend"] == 93.70
        assert cmp001["total_conversions"] == 27
        assert cmp001["CTR"] == round(640 / 26000, 4)
        assert cmp001["CPA"] == round(93.70 / 27, 2)

        cmp002 = next(r for r in results if r["campaign_id"] == "CMP002")
        assert cmp002["total_impressions"] == 8000
        assert cmp002["total_clicks"] == 120
        assert cmp002["total_spend"] == 28.00
        assert cmp002["total_conversions"] == 4

    def test_empty_csv(self, tmp_path):
        """A CSV with only a header row should return an empty list."""
        filepath = _create_csv(tmp_path, "empty.csv", [])
        results = process_csv(filepath)
        assert results == []

    def test_zero_conversions(self, tmp_path):
        """Campaigns with 0 conversions should have CPA = None."""
        filepath = _create_csv(tmp_path, "zero_conv.csv", [
            "CMP001,2025-01-01,10000,500,100.00,0",
        ])
        results = process_csv(filepath)
        assert len(results) == 1
        assert results[0]["CPA"] is None
        assert results[0]["CTR"] == round(500 / 10000, 4)

    def test_zero_impressions(self, tmp_path):
        """Campaigns with 0 impressions should have CTR = 0."""
        filepath = _create_csv(tmp_path, "zero_imp.csv", [
            "CMP001,2025-01-01,0,0,50.00,5",
        ])
        results = process_csv(filepath)
        assert len(results) == 1
        assert results[0]["CTR"] == 0.0
        assert results[0]["CPA"] == 10.0

    def test_malformed_rows_skipped(self, tmp_path):
        """Malformed rows should be skipped; valid rows still processed."""
        filepath = _create_csv(tmp_path, "malformed.csv", [
            "CMP001,2025-01-01,10000,500,100.00,10",
            "CMP002,2025-01-01,bad_number,500,100.00,10",  # bad impressions
            "CMP003,2025-01-01,10000,500,100.00,-5",        # negative conversions
        ])
        results = process_csv(filepath)
        # Only CMP001 should be in results (CMP002 bad number, CMP003 negative)
        assert len(results) == 1
        assert results[0]["campaign_id"] == "CMP001"

    def test_missing_columns(self, tmp_path):
        """CSV missing required columns should raise ValueError."""
        filepath = os.path.join(str(tmp_path), "bad_header.csv")
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            f.write("campaign_id,date,impressions\n")
            f.write("CMP001,2025-01-01,10000\n")

        with pytest.raises(ValueError, match="missing required columns"):
            process_csv(filepath)


class TestWriters:
    """Tests for core.writer functions."""

    def _make_campaigns(self, count):
        """Generate a list of mock campaign dicts."""
        campaigns = []
        for i in range(1, count + 1):
            campaigns.append({
                "campaign_id": f"CMP{i:03d}",
                "total_impressions": 10000 * i,
                "total_clicks": 100 * i,
                "total_spend": 50.0 * i,
                "total_conversions": 10 * i,
                "CTR": round((100 * i) / (10000 * i), 4),  # all same CTR: 0.01
                "CPA": round((50.0 * i) / (10 * i), 2),     # all same CPA: 5.0
            })
        return campaigns

    def test_top10_ctr_output(self, tmp_path):
        """Should write a CSV with top 10 campaigns sorted by CTR desc."""
        campaigns = []
        for i in range(1, 16):
            campaigns.append({
                "campaign_id": f"CMP{i:03d}",
                "total_impressions": 100000,
                "total_clicks": i * 100,      # CTR varies
                "total_spend": 1000.0,
                "total_conversions": 100,
                "CTR": round(i * 100 / 100000, 4),
                "CPA": 10.0,
            })

        output_dir = str(tmp_path)
        write_top10_ctr(campaigns, output_dir)

        output_path = os.path.join(output_dir, "top10_ctr.csv")
        assert os.path.exists(output_path)

        with open(output_path, "r", encoding="utf-8") as f:
            reader = list(csv.DictReader(f))

        assert len(reader) == 10
        # Highest CTR first (CMP015 has highest clicks)
        assert reader[0]["campaign_id"] == "CMP015"
        assert reader[9]["campaign_id"] == "CMP006"

    def test_top10_cpa_excludes_zero_conversions(self, tmp_path):
        """Should exclude campaigns with CPA=None (0 conversions)."""
        campaigns = [
            {
                "campaign_id": "CMP001",
                "total_impressions": 10000,
                "total_clicks": 100,
                "total_spend": 500.0,
                "total_conversions": 0,
                "CTR": 0.01,
                "CPA": None,  # 0 conversions
            },
            {
                "campaign_id": "CMP002",
                "total_impressions": 10000,
                "total_clicks": 100,
                "total_spend": 200.0,
                "total_conversions": 20,
                "CTR": 0.01,
                "CPA": 10.0,
            },
        ]

        output_dir = str(tmp_path)
        write_top10_cpa(campaigns, output_dir)

        output_path = os.path.join(output_dir, "top10_cpa.csv")
        with open(output_path, "r", encoding="utf-8") as f:
            reader = list(csv.DictReader(f))

        assert len(reader) == 1
        assert reader[0]["campaign_id"] == "CMP002"

    def test_top10_cpa_sorted_ascending(self, tmp_path):
        """CPA should be sorted ascending (lowest CPA first)."""
        campaigns = []
        for i in range(1, 16):
            campaigns.append({
                "campaign_id": f"CMP{i:03d}",
                "total_impressions": 100000,
                "total_clicks": 1000,
                "total_spend": 100.0 * i,  # CPA varies
                "total_conversions": 100,
                "CTR": 0.01,
                "CPA": round(100.0 * i / 100, 2),  # 1.0, 2.0, ..., 15.0
            })

        output_dir = str(tmp_path)
        write_top10_cpa(campaigns, output_dir)

        output_path = os.path.join(output_dir, "top10_cpa.csv")
        with open(output_path, "r", encoding="utf-8") as f:
            reader = list(csv.DictReader(f))

        assert len(reader) == 10
        # Lowest CPA first
        assert reader[0]["campaign_id"] == "CMP001"
        assert reader[9]["campaign_id"] == "CMP010"
