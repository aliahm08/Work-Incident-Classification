"""
Data processing and transformation utilities.
"""
import pandas as pd
from typing import Dict, List, Optional, Any
import re

from logger import get_logger

logger = get_logger(__name__)


class DataProcessor:
    """Processes and transforms data after reading."""
    
    def __init__(self):
        self.processing_steps = []
    
    def clean_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean column names: lowercase, replace spaces, remove special chars.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with cleaned column names
        """
        df = df.copy()
        
        # Clean column names
        new_columns = []
        for col in df.columns:
            # Convert to lowercase and replace spaces with underscores
            clean_col = str(col).lower().strip()
            clean_col = re.sub(r'\s+', '_', clean_col)
            # Remove special characters except underscores
            clean_col = re.sub(r'[^\w_]', '', clean_col)
            # Remove multiple underscores
            clean_col = re.sub(r'_+', '_', clean_col)
            # Remove leading/trailing underscores
            clean_col = clean_col.strip('_')
            
            new_columns.append(clean_col if clean_col else f'col_{len(new_columns)}')
        
        df.columns = new_columns
        logger.debug(f"Cleaned column names: {list(df.columns)}")
        return df
    
    def standardize_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize data types in the DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with standardized data types
        """
        df = df.copy()
        
        for col in df.columns:
            # Skip metadata columns
            if col.startswith('source_') or col.endswith('_timestamp'):
                continue
                
            # Try to convert numeric columns
            if df[col].dtype == 'object':
                # Try to convert to numeric
                numeric_converted = pd.to_numeric(df[col], errors='ignore')
                if not numeric_converted.equals(df[col]):
                    df[col] = numeric_converted
                    continue
                
                # Try to convert to datetime
                if 'date' in col.lower() or 'time' in col.lower():
                    datetime_converted = pd.to_datetime(df[col], errors='ignore')
                    if not datetime_converted.equals(df[col]):
                        df[col] = datetime_converted
        
        return df
    
    def remove_empty_rows_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove completely empty rows and columns.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with empty rows/columns removed
        """
        original_shape = df.shape
        
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Remove completely empty columns
        df = df.dropna(axis=1, how='all')
        
        new_shape = df.shape
        if new_shape != original_shape:
            logger.info(f"Removed empty rows/columns: {original_shape} -> {new_shape}")
        
        return df
    
    def extract_fleet_info(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """
        Extract fleet information from filename or data.
        
        Args:
            df: Input DataFrame
            source_file: Source filename
            
        Returns:
            DataFrame with fleet information added
        """
        df = df.copy()
        
        # Try to extract fleet number from filename
        fleet_match = re.search(r'fleet\s*(\d+)', source_file.lower())
        if fleet_match:
            fleet_number = fleet_match.group(1)
            df['fleet_number'] = fleet_number
            logger.debug(f"Extracted fleet number {fleet_number} from filename")
        else:
            df['fleet_number'] = None
        
        # Try to extract bus number range
        bus_range_match = re.search(r'(\d{4})-(\d{4})', source_file)
        if bus_range_match:
            start_bus = bus_range_match.group(1)
            end_bus = bus_range_match.group(2)
            df['bus_range_start'] = start_bus
            df['bus_range_end'] = end_bus
            logger.debug(f"Extracted bus range {start_bus}-{end_bus} from filename")
        
        return df
    
    def process_dataframe(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """
        Apply all processing steps to a DataFrame.
        
        Args:
            df: Input DataFrame
            source_file: Source filename for metadata extraction
            
        Returns:
            Processed DataFrame
        """
        logger.info(f"Processing DataFrame with {len(df)} rows, {len(df.columns)} columns")
        
        # Apply processing steps
        df = self.remove_empty_rows_columns(df)
        df = self.clean_column_names(df)
        df = self.standardize_data_types(df)
        df = self.extract_fleet_info(df, source_file)
        
        # Enforce string type for all object columns to prevent mixed-type issues in Parquet
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype("string")
            
        logger.info(f"Processed DataFrame: {len(df)} rows, {len(df.columns)} columns")
        return df


class FleetDataProcessor(DataProcessor):
    """Specialized processor for fleet performance data."""
    
    def identify_performance_metrics(self, df: pd.DataFrame) -> List[str]:
        """
        Identify columns that are likely performance metrics.
        
        Args:
            df: Input DataFrame
            
        Returns:
            List of metric column names
        """
        metric_columns = []
        
        # Common performance metric keywords
        metric_keywords = [
            'performance', 'efficiency', 'utilization', 'availability',
            'mileage', 'hours', 'cost', 'maintenance', 'downtime',
            'fuel', 'emissions', 'reliability', 'rating'
        ]
        
        for col in df.columns:
            if any(keyword in col.lower() for keyword in metric_keywords):
                metric_columns.append(col)
        
        return metric_columns
    
    def calculate_summary_stats(self, df: pd.DataFrame, group_by: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        Calculate summary statistics for performance metrics.
        
        Args:
            df: Input DataFrame
            group_by: Column to group by (e.g., fleet_number)
            
        Returns:
            Dictionary of summary statistics DataFrames
        """
        metric_columns = self.identify_performance_metrics(df)
        if not metric_columns:
            logger.warning("No performance metric columns identified")
            return {}
        
        summaries = {}
        
        # Overall summary
        numeric_df = df[metric_columns].select_dtypes(include=['number'])
        if not numeric_df.empty:
            summaries['overall'] = numeric_df.describe()
        
        # Grouped summary
        if group_by and group_by in df.columns:
            grouped = df.groupby(group_by)[metric_columns]
            summaries['by_group'] = grouped.describe()
        
        return summaries
