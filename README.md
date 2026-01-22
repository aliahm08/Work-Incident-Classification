# WMATA Bus Overhaul Data Analytics Pipeline

A comprehensive data ingestion pipeline for processing WMATA bus fleet performance and overhaul data.

## Overview

This pipeline automatically discovers, processes, and consolidates Excel files containing bus fleet data from multiple sources. It handles data cleaning, validation, and transformation to prepare the data for analysis.

## Features

- **Automatic File Discovery**: Scans directories for Excel files and groups them by fleet
- **Multi-Sheet Processing**: Handles Excel files with multiple data sheets
- **Data Cleaning**: Standardizes column names, data types, and removes empty rows/columns
- **Fleet Intelligence**: Extracts fleet numbers and bus ranges from filenames and data
- **Data Validation**: Built-in validation rules to ensure data quality
- **Multiple Output Formats**: Supports Parquet, CSV, and Excel output formats
- **Partitioned Output**: Creates partitioned datasets by fleet for efficient querying
- **Summary Statistics**: Generates automatic summary reports for each fleet
- **Comprehensive Logging**: Detailed logging for monitoring and debugging

## Installation

1. Install required dependencies:
```bash
pip install -r requirements.txt
```

2. The pipeline is ready to run!

## Usage

### Basic Usage

Run the complete pipeline with default settings:

```bash
python main.py
```

This will:
- Scan the `raw-import-from-client/` and `source/` directories
- Process all Excel files found
- Generate cleaned, consolidated datasets
- Create summary reports
- Save results to the `processed/` directory

### Advanced Usage

```python
from pipeline import DataIngestionPipeline
from pathlib import Path

# Initialize with custom output directory
pipeline = DataIngestionPipeline(output_dir=Path("custom_output"))

# Run the pipeline
results = pipeline.run_full_pipeline("fleet_performance")

# Access results
print(f"Processed {results['stats']['files_processed']} files")
print(f"Total rows: {results['stats']['total_rows']}")
```

## Directory Structure

```
WMATA-bus-overhaul-data-analytics/
├── main.py                    # Main entry point
├── requirements.txt           # Python dependencies
├── README.md                  # This file
├── pipeline/                  # Pipeline modules
│   ├── config.py             # Configuration settings
│   ├── logger.py             # Logging utilities
│   ├── data_sources.py       # File discovery and management
│   ├── readers.py            # Data readers (Excel, CSV, etc.)
│   ├── processors.py         # Data processing and transformation
│   ├── writers.py            # Data output utilities
│   ├── validators.py         # Data validation
│   └── pipeline.py           # Main pipeline orchestrator
├── raw-import-from-client/   # Raw data files from client
├── source/                   # Additional source files
├── processed/                # Processed output (created automatically)
└── logs/                     # Pipeline logs (created automatically)
```

## Input Data Format

The pipeline expects Excel files (`.xlsx`, `.xls`) containing fleet performance data. Files should be organized in directories, preferably with fleet information in the directory or filename structure.

Example naming convention:
- `Fleet 56 7300-7355/performance_data.xlsx`
- `Fleet 57 7356-7409/maintenance_records.xlsx`

## Output

The pipeline generates several types of output:

### Processed Data Files
- **Fleet-specific files**: `{fleet_name}_combined.parquet` - Combined data for each fleet
- **Consolidated dataset**: `consolidated_fleet_data.parquet` - All data in one file
- **Partitioned data**: `partitioned/fleet_number={fleet}/fleet_data.parquet` - Data partitioned by fleet

### Summary Reports
- **Fleet summaries**: `{fleet_name}_summary.xlsx` - Statistics for each fleet
- **Performance metrics**: Automatic calculation of key performance indicators

### Logs
- **Pipeline logs**: `logs/pipeline.log` - Detailed execution logs

## Configuration

Key configuration options in `pipeline/config.py`:

```python
# Data source directories
RAW_DATA_DIR = BASE_DIR / "raw-import-from-client"
SOURCE_DATA_DIR = BASE_DIR / "source"
PROCESSED_DATA_DIR = BASE_DIR / "processed"

# Processing options
PROCESSING_CONFIG = {
    "excel": {
        "engine": "openpyxl",
        "skip_rows": 0,
        "na_values": ["", "NA", "N/A", "null", "NULL"]
    },
    "output": {
        "format": "parquet",  # or "csv", "excel"
        "compression": "snappy"
    }
}
```

## Data Validation

The pipeline includes built-in validation rules:

- **Required columns**: Checks for metadata columns
- **Data completeness**: Validates that DataFrames contain data
- **Column naming**: Ensures valid column names
- **Duplicate detection**: Identifies excessive duplicates
- **Fleet number validation**: Checks fleet number format and range
- **Data type validation**: Ensures reasonable data type distribution

## Error Handling

The pipeline is designed to be robust:
- Continues processing even if individual files fail
- Logs all errors and warnings
- Provides detailed error reporting
- Generates partial results when possible

## Performance

- **Efficient processing**: Uses pandas and PyArrow for optimal performance
- **Parallel-ready**: Architecture supports future parallel processing
- **Memory-conscious**: Processes files individually to manage memory usage
- **Compressed output**: Uses Snappy compression for efficient storage

## Extending the Pipeline

### Adding New Data Sources

1. Update `DATA_SOURCES` in `config.py`
2. Add new reader methods in `readers.py` if needed
3. Create specialized processors in `processors.py`

### Adding Validation Rules

```python
from validators import FleetDataValidator

validator = FleetDataValidator()

def custom_rule(df, context):
    # Your validation logic
    return True, "Validation passed"

validator.add_rule("custom_rule", custom_rule, "Custom validation rule")
```

### Custom Processing

```python
from processors import FleetDataProcessor

class CustomProcessor(FleetDataProcessor):
    def custom_transformation(self, df):
        # Your custom processing
        return df
```

## Troubleshooting

### Common Issues

1. **File not found**: Check that input directories exist and contain Excel files
2. **Permission errors**: Ensure read/write permissions for all directories
3. **Memory issues**: Process large files individually or increase available memory
4. **Excel parsing errors**: Verify Excel files are not corrupted or password-protected

### Debug Mode

Enable debug logging by modifying `config.py`:

```python
LOGGING_CONFIG = {
    "level": "DEBUG",  # Change from "INFO" to "DEBUG"
    # ... rest of config
}
```

## Contributing

When contributing to the pipeline:
1. Follow the existing code style
2. Add comprehensive logging
3. Include validation for new features
4. Update documentation
5. Test with sample data

## License

This project is part of the WMATA Bus Overhaul Data Analytics initiative.
# Work-Incident-Classification
