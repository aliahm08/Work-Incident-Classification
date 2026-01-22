"""
Main data ingestion pipeline orchestrator.
"""
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

from config import PROCESSED_DATA_DIR
from logger import setup_logger
from data_sources import DataSourceManager
from readers import DataReader
from processors import FleetDataProcessor
from writers import DataWriter, PartitionedWriter
from validators import FleetDataValidator

logger = setup_logger("pipeline")


class DataIngestionPipeline:
    """Main pipeline for ingesting and processing fleet data."""
    
    def __init__(self, output_dir: Optional[Path] = None):
        self.output_dir = output_dir or PROCESSED_DATA_DIR
        self.source_manager = DataSourceManager()
        self.reader = DataReader()
        self.processor = FleetDataProcessor()
        self.validator = FleetDataValidator()
        self.writer = DataWriter(self.output_dir)
        self.partitioned_writer = PartitionedWriter(self.output_dir / "partitioned")
        self.last_consolidated_df = None
        
        # Track pipeline state
        self.processing_stats = {
            "files_processed": 0,
            "sheets_processed": 0,
            "total_rows": 0,
            "errors": [],
            "validation_failures": 0
        }
    
    def run_full_pipeline(self, source_type: str = "fleet_performance") -> Dict:
        """
        Run the complete data ingestion pipeline.
        
        Args:
            source_type: Type of data source to process
            
        Returns:
            Dictionary with processing results and statistics
        """
        logger.info(f"Starting full pipeline for {source_type}")
        start_time = datetime.now()
        
        try:
            # Discover files
            files = self.source_manager.discover_files(source_type)
            if not files:
                logger.warning(f"No files found for {source_type}")
                return {"status": "no_files", "stats": self.processing_stats}
            
            # Group files by fleet
            fleet_groups = self.source_manager.group_files_by_fleet(files)
            logger.info(f"Found {len(fleet_groups)} fleet groups")
            
            # Process each fleet
            results = {}
            for fleet_name, fleet_files in fleet_groups.items():
                logger.info(f"Processing fleet: {fleet_name}")
                fleet_result = self.process_fleet_files(fleet_name, fleet_files)
                results[fleet_name] = fleet_result
            
            # Create consolidated dataset
            consolidated_result = self.create_consolidated_dataset(results)
            
            # Generate final report
            end_time = datetime.now()
            processing_time = end_time - start_time
            
            final_results = {
                "status": "completed",
                "processing_time": str(processing_time),
                "fleets_processed": len(results),
                "stats": self.processing_stats,
                "validation_summary": self.validator.get_validation_summary(),
                "fleet_results": results,
                "consolidated": consolidated_result
            }
            
            logger.info(f"Pipeline completed in {processing_time}")
            return final_results
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            self.processing_stats["errors"].append(str(e))
            return {
                "status": "failed",
                "error": str(e),
                "stats": self.processing_stats
            }
    
    def process_fleet_files(self, fleet_name: str, files: List[Path]) -> Dict:
        """
        Process all files for a specific fleet.
        
        Args:
            fleet_name: Name of the fleet
            files: List of files for this fleet
            
        Returns:
            Dictionary with processing results for this fleet
        """
        fleet_data = []
        fleet_summaries = {}
        
        for file_path in files:
            try:
                file_result = self.process_single_file(file_path, fleet_name)
                if file_result["data"] is not None:
                    fleet_data.extend(file_result["data"])
                if file_result["summaries"]:
                    fleet_summaries.update(file_result["summaries"])
                
                self.processing_stats["files_processed"] += 1
                
            except Exception as e:
                logger.error(f"Error processing {file_path}: {str(e)}")
                self.processing_stats["errors"].append(f"{file_path}: {str(e)}")
        
        # Combine all fleet data
        if fleet_data:
            combined_df = pd.concat(fleet_data, ignore_index=True)
            
            # Write combined fleet data
            fleet_output_path = self.writer.write_dataframe(
                combined_df, 
                f"{fleet_name}_combined"
            )
            
            # Write fleet summary
            if fleet_summaries:
                summary_path = self.writer.write_summary_report(fleet_summaries, fleet_name)
            else:
                summary_path = None
            
            return {
                "status": "completed",
                "rows_processed": len(combined_df),
                "output_path": str(fleet_output_path),
                "summary_path": str(summary_path) if summary_path else None,
                "data": [combined_df]
            }
        else:
            return {
                "status": "no_data",
                "data": []
            }
    
    def process_single_file(self, file_path: Path, fleet_name: str) -> Dict:
        """
        Process a single file and extract all sheets.
        
        Args:
            file_path: Path to the file
            fleet_name: Name of the fleet
            
        Returns:
            Dictionary with processing results
        """
        logger.info(f"Processing file: {file_path.name}")
        
        # Get file metadata
        file_metadata = self.reader.get_file_metadata(file_path)
        
        # Read all sheets
        excel_data = self.reader.read_file(file_path)
        
        processed_sheets = []
        summaries = {}
        validation_results = []
        
        if isinstance(excel_data, dict):
            # Multiple sheets
            for sheet_name, sheet_df in excel_data.items():
                if sheet_df.empty:
                    continue
                
                # Process the sheet
                processed_df = self.processor.process_dataframe(sheet_df, file_path.name)
                processed_sheets.append(processed_df)

                validation_results.append(
                    self.validator.validate_dataframe(
                        processed_df,
                        context={"file": file_path.name, "sheet": sheet_name, "fleet": fleet_name}
                    )
                )
                if not validation_results[-1]["passed"]:
                    self.processing_stats["validation_failures"] += 1
                
                # Calculate summary statistics
                sheet_summaries = self.processor.calculate_summary_stats(
                    processed_df, 
                    group_by='fleet_number'
                )
                if sheet_summaries:
                    summaries[f"{sheet_name}_summary"] = sheet_summaries
                
                self.processing_stats["sheets_processed"] += 1
                self.processing_stats["total_rows"] += len(processed_df)
        else:
            # Single sheet
            processed_df = self.processor.process_dataframe(excel_data, file_path.name)
            processed_sheets.append(processed_df)

            validation_results.append(
                self.validator.validate_dataframe(
                    processed_df,
                    context={"file": file_path.name, "sheet": "<single>", "fleet": fleet_name}
                )
            )
            if not validation_results[-1]["passed"]:
                self.processing_stats["validation_failures"] += 1
            
            sheet_summaries = self.processor.calculate_summary_stats(
                processed_df, 
                group_by='fleet_number'
            )
            if sheet_summaries:
                summaries["summary"] = sheet_summaries
            
            self.processing_stats["sheets_processed"] += 1
            self.processing_stats["total_rows"] += len(processed_df)
        
        return {
            "file_metadata": file_metadata,
            "data": processed_sheets,
            "summaries": summaries,
            "validation_results": validation_results
        }
    
    def create_consolidated_dataset(self, fleet_results: Dict) -> Dict:
        """
        Create a consolidated dataset from all fleet results.
        
        Args:
            fleet_results: Results from each fleet processing
            
        Returns:
            Dictionary with consolidated dataset info
        """
        all_data = []
        
        for fleet_name, fleet_result in fleet_results.items():
            if fleet_result.get("data"):
                all_data.extend(fleet_result["data"])
        
        if all_data:
            # Combine all data
            consolidated_df = pd.concat(all_data, ignore_index=True)
            self.last_consolidated_df = consolidated_df

            # Write consolidated dataset
            consolidated_path = self.writer.write_dataframe(
                consolidated_df, 
                "consolidated_fleet_data"
            )

            analysis_summary = self._build_analysis_summary(consolidated_df)
            
            # Also create partitioned version by fleet
            if 'fleet_number' in consolidated_df.columns:
                partitioned_files = self.partitioned_writer.write_partitioned(
                    consolidated_df,
                    ['fleet_number'],
                    "fleet_data"
                )
            else:
                partitioned_files = []
            
            return {
                "status": "completed",
                "total_rows": len(consolidated_df),
                "consolidated_path": str(consolidated_path),
                "partitioned_files": [str(f) for f in partitioned_files],
                "analysis_summary": analysis_summary
            }
        else:
            return {
                "status": "no_data"
            }
    
    def get_processing_summary(self) -> Dict:
        """Get a summary of processing statistics."""
        return self.processing_stats.copy()

    def _build_analysis_summary(self, df: pd.DataFrame) -> Dict:
        """Build a compact analysis summary for reporting."""
        summary = {
            "row_count": len(df),
            "column_count": len(df.columns)
        }

        if "fleet_number" in df.columns:
            summary["fleet_counts"] = df["fleet_number"].value_counts(dropna=False).to_dict()

        numeric_df = df.select_dtypes(include=["number"])
        if not numeric_df.empty:
            stats = numeric_df.describe().loc[["count", "mean", "min", "max"]].round(2)
            summary["numeric_summary"] = stats.to_dict(orient="dict")

        return summary

    def export_consolidated_csv(self, filename: str = "consolidated_fleet_data") -> Path:
        """Export the most recent consolidated dataset to CSV."""
        if self.last_consolidated_df is None:
            raise ValueError("No consolidated data available. Run the pipeline first.")
        return self.writer.write_dataframe(self.last_consolidated_df, filename, format_type="csv")
