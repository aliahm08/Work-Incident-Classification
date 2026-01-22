"""
Data writers for different output formats.
"""
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional

from config import PROCESSING_CONFIG
from logger import get_logger

logger = get_logger(__name__)


class DataWriter:
    """Writes processed data to various formats."""
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True)
        self.config = PROCESSING_CONFIG["output"]
    
    def write_dataframe(self, df: pd.DataFrame, filename: str, format_type: Optional[str] = None) -> Path:
        """
        Write a DataFrame to file.
        
        Args:
            df: DataFrame to write
            filename: Output filename
            format_type: Output format (parquet, csv, excel)
            
        Returns:
            Path to written file
        """
        format_type = format_type or self.config["format"]
        output_path = self.output_dir / filename
        
        try:
            if format_type.lower() == "parquet":
                df = df.copy()
                for col in df.columns:
                    if df[col].dtype == 'object':
                        df[col] = df[col].astype("string")
                
                output_path = output_path.with_suffix(".parquet")
                df.to_parquet(
                    output_path,
                    compression=self.config["compression"],
                    index=False
                )
            elif format_type.lower() == "csv":
                output_path = output_path.with_suffix(".csv")
                df.to_csv(output_path, index=False)
            elif format_type.lower() == "excel":
                output_path = output_path.with_suffix(".xlsx")
                df.to_excel(output_path, index=False)
            else:
                raise ValueError(f"Unsupported format: {format_type}")
            
            logger.info(f"Wrote {len(df)} rows to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error writing to {output_path}: {str(e)}")
            raise
    
    def write_multiple_sheets(self, data_dict: Dict[str, pd.DataFrame], filename: str) -> Path:
        """
        Write multiple DataFrames to separate sheets in an Excel file.
        
        Args:
            data_dict: Dictionary mapping sheet names to DataFrames
            filename: Output filename
            
        Returns:
            Path to written file
        """
        output_path = self.output_dir / f"{filename}.xlsx"
        
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                for sheet_name, df in data_dict.items():
                    # Clean sheet name for Excel compatibility
                    clean_sheet_name = str(sheet_name)[:31]  # Excel limit
                    df.to_excel(writer, sheet_name=clean_sheet_name, index=False)
            
            total_rows = sum(len(df) for df in data_dict.values())
            logger.info(f"Wrote {len(data_dict)} sheets ({total_rows} total rows) to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error writing Excel file {output_path}: {str(e)}")
            raise
    
    def write_summary_report(self, summaries: Dict[str, pd.DataFrame], fleet_name: str) -> Path:
        """
        Write summary statistics report.
        
        Args:
            summaries: Dictionary of summary DataFrames
            fleet_name: Name of the fleet
            
        Returns:
            Path to written report
        """
        report_data = {}
        
        for summary_type, summary_df in summaries.items():
            if isinstance(summary_df, pd.DataFrame):
                # Flatten multi-level columns if present
                if isinstance(summary_df.columns, pd.MultiIndex):
                    summary_df.columns = ['_'.join(col).strip() for col in summary_df.columns.values]
                report_data[summary_type] = summary_df
        
        if report_data:
            return self.write_multiple_sheets(report_data, f"{fleet_name}_summary")
        else:
            logger.warning(f"No summary data to write for {fleet_name}")
            return None


class PartitionedWriter:
    """Writes data partitioned by specified columns."""
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True)
    
    def write_partitioned(self, df: pd.DataFrame, partition_columns: List[str], filename_template: str) -> List[Path]:
        """
        Write DataFrame partitioned by specified columns.
        
        Args:
            df: DataFrame to write
            partition_columns: Columns to partition by
            filename_template: Template for output filenames
            
        Returns:
            List of paths to written files
        """
        written_files = []
        
        # Group by partition columns
        grouped = df.groupby(partition_columns)
        
        for group_values, group_df in grouped:
            # Create partition directory structure
            partition_path = self.output_dir
            for col, val in zip(partition_columns, group_values):
                partition_path = partition_path / f"{col}={val}"
            
            partition_path.mkdir(parents=True, exist_ok=True)
            
            # Generate filename
            if isinstance(group_values, tuple):
                group_str = "_".join(str(v) for v in group_values)
            else:
                group_str = str(group_values)
            
            filename = filename_template.format(group=group_str)
            output_path = partition_path / f"{filename}.parquet"
            
            # Write partition
            group_df.to_parquet(output_path, index=False, compression="snappy")
            written_files.append(output_path)
            
            logger.debug(f"Wrote partition {group_str}: {len(group_df)} rows to {output_path}")
        
        logger.info(f"Wrote {len(written_files)} partitions for {len(df)} total rows")
        return written_files
