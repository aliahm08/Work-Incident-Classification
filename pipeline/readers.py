"""
Data readers for different file formats.
"""
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Union

from config import PROCESSING_CONFIG
from logger import get_logger

logger = get_logger(__name__)


class ExcelReader:
    """Reader for Excel files with multiple sheets."""
    
    def __init__(self):
        self.config = PROCESSING_CONFIG["excel"]

    def _get_engine(self, file_path: Path) -> str:
        """Select the correct engine based on the file extension."""
        if file_path.suffix.lower() == ".xls":
            return self.config.get("xls_engine", self.config["engine"])
        return self.config["engine"]
    
    def read_file(self, file_path: Path, sheet_name: Optional[Union[str, int]] = None) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """
        Read Excel file(s).
        
        Args:
            file_path: Path to Excel file
            sheet_name: Specific sheet to read, or None for all sheets
            
        Returns:
            DataFrame or dictionary of DataFrames (one per sheet)
        """
        try:
            if sheet_name is None:
                # Read all sheets
                excel_data = pd.read_excel(
                    file_path,
                    engine=self._get_engine(file_path),
                    skiprows=self.config["skip_rows"],
                    na_values=self.config["na_values"],
                    keep_default_na=self.config["keep_default_na"],
                    sheet_name=None  # Read all sheets
                )
                logger.info(f"Read {len(excel_data)} sheets from {file_path.name}")
                return excel_data
            else:
                # Read specific sheet
                df = pd.read_excel(
                    file_path,
                    engine=self._get_engine(file_path),
                    skiprows=self.config["skip_rows"],
                    na_values=self.config["na_values"],
                    keep_default_na=self.config["keep_default_na"],
                    sheet_name=sheet_name
                )
                logger.info(f"Read sheet '{sheet_name}' from {file_path.name}: {len(df)} rows")
                return df
                
        except Exception as e:
            logger.error(f"Error reading {file_path}: {str(e)}")
            raise
    
    def get_sheet_names(self, file_path: Path) -> List[str]:
        """
        Get all sheet names from an Excel file.
        
        Args:
            file_path: Path to Excel file
            
        Returns:
            List of sheet names
        """
        try:
            excel_file = pd.ExcelFile(file_path, engine=self._get_engine(file_path))
            return excel_file.sheet_names
        except Exception as e:
            logger.error(f"Error getting sheet names from {file_path}: {str(e)}")
            return []
    
    def read_sheet_with_metadata(self, file_path: Path, sheet_name: str) -> pd.DataFrame:
        """
        Read a sheet and add metadata columns.
        
        Args:
            file_path: Path to Excel file
            sheet_name: Sheet name to read
            
        Returns:
            DataFrame with added metadata columns
        """
        df = self.read_file(file_path, sheet_name)
        
        # Add metadata columns
        df['source_file'] = file_path.name
        df['source_sheet'] = sheet_name
        df['source_path'] = str(file_path)
        df['ingestion_timestamp'] = pd.Timestamp.now()
        
        return df


class DataReader:
    """Main data reader that delegates to format-specific readers."""
    
    def __init__(self):
        self.excel_reader = ExcelReader()
    
    def read_file(self, file_path: Path, **kwargs) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """
        Read a file based on its extension.
        
        Args:
            file_path: Path to file
            **kwargs: Additional arguments for specific readers
            
        Returns:
            DataFrame or dictionary of DataFrames
        """
        suffix = file_path.suffix.lower()
        
        if suffix in ['.xlsx', '.xls']:
            return self.excel_reader.read_file(file_path, **kwargs)
        else:
            raise ValueError(f"Unsupported file format: {suffix}")
    
    def get_file_metadata(self, file_path: Path) -> Dict:
        """
        Get metadata about a file.
        
        Args:
            file_path: Path to file
            
        Returns:
            Dictionary with file metadata
        """
        metadata = {
            'file_name': file_path.name,
            'file_path': str(file_path),
            'file_size': file_path.stat().st_size,
            'file_extension': file_path.suffix.lower()
        }
        
        # Add Excel-specific metadata
        if file_path.suffix.lower() in ['.xlsx', '.xls']:
            sheet_names = self.excel_reader.get_sheet_names(file_path)
            metadata['sheet_names'] = sheet_names
            metadata['sheet_count'] = len(sheet_names)
        
        return metadata
