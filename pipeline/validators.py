"""
Data validation utilities for the ingestion pipeline.
"""
import pandas as pd
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path

from logger import get_logger

logger = get_logger(__name__)


class DataValidator:
    """Validates data quality and integrity."""
    
    def __init__(self):
        self.validation_rules = {}
        self.validation_results = []
    
    def add_rule(self, name: str, rule_func: callable, description: str = ""):
        """
        Add a validation rule.
        
        Args:
            name: Rule name
            rule_func: Function that takes a DataFrame and returns (bool, str)
            description: Rule description
        """
        self.validation_rules[name] = {
            "func": rule_func,
            "description": description
        }
    
    def validate_dataframe(self, df: pd.DataFrame, context: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Validate a DataFrame against all rules.
        
        Args:
            df: DataFrame to validate
            context: Additional context for validation
            
        Returns:
            Dictionary with validation results
        """
        context = context or {}
        results = {
            "context": context,
            "shape": df.shape,
            "columns": list(df.columns),
            "rules": {},
            "passed": True,
            "warnings": [],
            "errors": []
        }
        
        for rule_name, rule_info in self.validation_rules.items():
            try:
                passed, message = rule_info["func"](df, context)
                results["rules"][rule_name] = {
                    "passed": passed,
                    "message": message,
                    "description": rule_info["description"]
                }
                
                if not passed:
                    results["passed"] = False
                    if "error" in message.lower():
                        results["errors"].append(f"{rule_name}: {message}")
                    else:
                        results["warnings"].append(f"{rule_name}: {message}")
                        
            except Exception as e:
                logger.error(f"Validation rule {rule_name} failed: {str(e)}")
                results["rules"][rule_name] = {
                    "passed": False,
                    "message": f"Rule execution failed: {str(e)}",
                    "description": rule_info["description"]
                }
                results["passed"] = False
                results["errors"].append(f"{rule_name}: Rule execution failed")
        
        self.validation_results.append(results)
        return results
    
    def get_validation_summary(self) -> Dict[str, Any]:
        """Get summary of all validation results."""
        if not self.validation_results:
            return {"total_validations": 0}
        
        total = len(self.validation_results)
        passed = sum(1 for r in self.validation_results if r.get("passed", False))
        
        return {
            "total_validations": total,
            "passed": passed,
            "failed": total - passed,
            "pass_rate": passed / total if total > 0 else 0
        }


class FleetDataValidator(DataValidator):
    """Specialized validator for fleet data."""
    
    def __init__(self):
        super().__init__()
        self._setup_fleet_rules()
    
    def _setup_fleet_rules(self):
        """Set up validation rules specific to fleet data."""
        
        # Rule: Check for required columns
        def has_required_columns(df: pd.DataFrame, context: Dict) -> Tuple[bool, str]:
            required_cols = ['source_file', 'ingestion_timestamp']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                return False, f"Missing required columns: {missing_cols}"
            return True, "All required columns present"
        
        self.add_rule(
            "required_columns",
            has_required_columns,
            "Check that all required metadata columns are present"
        )
        
        # Rule: Check for data in DataFrame
        def has_data(df: pd.DataFrame, context: Dict) -> Tuple[bool, str]:
            if df.empty:
                return False, "DataFrame is empty"
            return True, f"DataFrame has {len(df)} rows"
        
        self.add_rule(
            "has_data",
            has_data,
            "Check that DataFrame contains data"
        )
        
        # Rule: Check for reasonable column names
        def has_valid_columns(df: pd.DataFrame, context: Dict) -> Tuple[bool, str]:
            invalid_cols = [col for col in df.columns if not str(col).strip()]
            if invalid_cols:
                return False, f"Invalid column names found: {invalid_cols}"
            return True, "All column names are valid"
        
        self.add_rule(
            "valid_columns",
            has_valid_columns,
            "Check that all column names are valid"
        )
        
        # Rule: Check for duplicate rows (allowing some duplicates)
        def reasonable_duplicates(df: pd.DataFrame, context: Dict) -> Tuple[bool, str]:
            if len(df) == 0:
                return True, "Empty DataFrame, no duplicates"
            
            duplicate_count = df.duplicated().sum()
            duplicate_rate = duplicate_count / len(df)
            
            if duplicate_rate > 0.5:  # More than 50% duplicates seems suspicious
                return False, f"High duplicate rate: {duplicate_rate:.2%}"
            elif duplicate_rate > 0.1:  # More than 10% duplicates is a warning
                return True, f"Moderate duplicate rate: {duplicate_rate:.2%}"
            else:
                return True, f"Low duplicate rate: {duplicate_rate:.2%}"
        
        self.add_rule(
            "reasonable_duplicates",
            reasonable_duplicates,
            "Check for reasonable duplicate row rates"
        )
        
        # Rule: Check for fleet number format
        def valid_fleet_numbers(df: pd.DataFrame, context: Dict) -> Tuple[bool, str]:
            if 'fleet_number' not in df.columns:
                return True, "No fleet_number column to validate"
            
            fleet_numbers = df['fleet_number'].dropna()
            if fleet_numbers.empty:
                return True, "No fleet numbers to validate"
            
            # Check if fleet numbers are reasonable (numeric, reasonable range)
            try:
                numeric_fleets = pd.to_numeric(fleet_numbers, errors='coerce')
                invalid_fleets = numeric_fleets.isna()
                if invalid_fleets.any():
                    return False, f"Invalid fleet number format in {invalid_fleets.sum()} rows"
                
                # Check reasonable range (1-999)
                out_of_range = (numeric_fleets < 1) | (numeric_fleets > 999)
                if out_of_range.any():
                    return False, f"Fleet numbers out of range (1-999): {out_of_range.sum()} rows"
                
                return True, f"All fleet numbers valid (range: {numeric_fleets.min()}-{numeric_fleets.max()})"
                
            except Exception as e:
                return False, f"Error validating fleet numbers: {str(e)}"
        
        self.add_rule(
            "valid_fleet_numbers",
            valid_fleet_numbers,
            "Check that fleet numbers are in valid format and range"
        )
        
        # Rule: Check for reasonable data types
        def reasonable_data_types(df: pd.DataFrame, context: Dict) -> Tuple[bool, str]:
            # Count columns by type
            type_counts = df.dtypes.value_counts().to_dict()
            
            # Too many object columns might indicate parsing issues
            object_cols = sum(1 for dtype in df.dtypes if dtype == 'object')
            total_cols = len(df.columns)
            object_ratio = object_cols / total_cols
            
            if object_ratio > 0.8:
                return False, f"High ratio of object columns: {object_ratio:.2%}"
            
            return True, f"Data types appear reasonable: {type_counts}"
        
        self.add_rule(
            "reasonable_data_types",
            reasonable_data_types,
            "Check for reasonable data type distribution"
        )


def validate_file_before_processing(file_path: Path) -> Tuple[bool, List[str]]:
    """
    Quick validation before attempting to read a file.
    
    Args:
        file_path: Path to file
        
    Returns:
        Tuple of (is_valid, list_of_issues)
    """
    issues = []
    
    # Check if file exists
    if not file_path.exists():
        issues.append(f"File does not exist: {file_path}")
        return False, issues
    
    # Check if file is readable
    if not file_path.stat().st_size > 0:
        issues.append(f"File is empty: {file_path}")
        return False, issues
    
    # Check file extension
    valid_extensions = ['.xlsx', '.xls']
    if file_path.suffix.lower() not in valid_extensions:
        issues.append(f"Unsupported file extension: {file_path.suffix}")
        return False, issues
    
    return True, issues
