"""
uitility.py
Utility functions for transforming parsed ENR records.
"""

from typing import List, Dict, Any, Optional
from pathlib import Path
import json
import logging



def add_location_field(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Add a 'location' field to each record, derived from the 'firm' field.

    If 'firm' contains a comma, text after the first comma is treated as location.
    If no comma is present or value is not a string, location is set to None.
    """
    for record in records:
        firm_value = record.get('firm')
        if isinstance(firm_value, str) and ',' in firm_value:
            _, location_part = firm_value.split(',', 1)
            record['location'] = location_part.strip()
        else:
            record['location'] = None
    return records

def filter_relevant_fields(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Keep only the required fields for each record.
    
    Missing fields are included with value None to keep a consistent schema.
    """
    keep = [
        "enr_rank_2025",
        "enr_rank_2024",
        "firm",
        "total_revenue_m",
        "int_total_revenue_m",
        "new_contracts",
        "general_building_pct",
        "water_supply_pct",
        "location"
    ]
    filtered: List[Dict[str, Any]] = []
    for rec in records:
        filtered.append({k: rec.get(k) for k in keep})
    return filtered


def save_stage_snapshot(
    records: List[Dict[str, Any]],
    filename: str,
    output_root: Optional[Path] = None,
) -> Path:
    """Save a JSON snapshot for a pipeline stage into the output folder.

    - records: list of dictionaries to persist
    - filename: file name like "step1_raw_records.json"
    - output_root: optional root directory; defaults to project_root/output
    Returns the full path to the written file.
    """
    logger = logging.getLogger("utility")
    root = output_root or (Path(__file__).parent / "output")
    root.mkdir(exist_ok=True)
    target_path = root / filename
    with open(target_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    logger.info("Saved snapshot: %s", str(target_path))
    return target_path


def clean_data(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """ Clens data """
    records = add_location_field(records)
    records = filter_relevant_fields(records)
    return records
