"""
Configuration module for the data ingestion pipeline.
"""
import os
from pathlib import Path
from typing import Dict, List

# Base paths
BASE_DIR = Path(__file__).parent.parent
RAW_DATA_DIR = BASE_DIR / "raw-import-from-client"
SOURCE_DATA_DIR = BASE_DIR / "source"
REFERENCES_DIR = BASE_DIR / "references"
PROCESSED_DATA_DIR = BASE_DIR / "processed"
LOGS_DIR = BASE_DIR / "logs"

# Create directories if they don't exist
for dir_path in [PROCESSED_DATA_DIR, LOGS_DIR]:
    dir_path.mkdir(exist_ok=True)

# File patterns and data sources
DATA_SOURCES = {
    "fleet_performance": {
        "patterns": ["**/*.xlsx", "**/*.xls"],
        "directories": [RAW_DATA_DIR, SOURCE_DATA_DIR, REFERENCES_DIR],
        "exclude_patterns": ["~$*", "*.tmp"]
    }
}

# Processing configuration
PROCESSING_CONFIG = {
    "excel": {
        "engine": "openpyxl",
        "xls_engine": "xlrd",
        "skip_rows": 0,
        "na_values": ["", "NA", "N/A", "null", "NULL"],
        "keep_default_na": True
    },
    "output": {
        "format": "parquet",
        "compression": "snappy"
    }
}

# Logging configuration
LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file": LOGS_DIR / "pipeline.log"
}
