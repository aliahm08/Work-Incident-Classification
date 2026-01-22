#!/usr/bin/env python3
"""
Main entry point for the WMATA bus overhaul data ingestion pipeline.
"""
import sys
from pathlib import Path

# Add pipeline directory to path
sys.path.insert(0, str(Path(__file__).parent / "pipeline"))

from pipeline import DataIngestionPipeline
from validators import FleetDataValidator
from logger import setup_logger

logger = setup_logger("main")


def main():
    """Main function to run the data ingestion pipeline."""
    logger.info("Starting WMATA Bus Overhaul Data Ingestion Pipeline")
    
    try:
        # Initialize pipeline
        pipeline = DataIngestionPipeline()
        
        # Run the full pipeline
        results = pipeline.run_full_pipeline("fleet_performance")
        
        # Print results summary
        print("\n" + "="*60)
        print("PIPELINE EXECUTION SUMMARY")
        print("="*60)
        print(f"Status: {results['status']}")
        
        if results['status'] == 'completed':
            print(f"Processing Time: {results['processing_time']}")
            print(f"Fleets Processed: {results['fleets_processed']}")
            print(f"Files Processed: {results['stats']['files_processed']}")
            print(f"Sheets Processed: {results['stats']['sheets_processed']}")
            print(f"Total Rows: {results['stats']['total_rows']}")
            print(f"Validation Failures: {results['stats'].get('validation_failures', 0)}")

            validation_summary = results.get("validation_summary", {})
            if validation_summary:
                total_validations = validation_summary.get("total_validations", 0)
                passed = validation_summary.get("passed", 0)
                failed = validation_summary.get("failed", 0)
                pass_rate = validation_summary.get("pass_rate", 0)
                print("\nValidation Summary:")
                print(f"  Total: {total_validations} | Passed: {passed} | Failed: {failed} | Pass Rate: {pass_rate:.2%}")
            
            if results['stats']['errors']:
                print(f"Errors: {len(results['stats']['errors'])}")
                for error in results['stats']['errors'][:5]:  # Show first 5 errors
                    print(f"  - {error}")
            
            print("\nOutput Files:")
            for fleet_name, fleet_result in results['fleet_results'].items():
                if fleet_result.get('output_path'):
                    print(f"  {fleet_name}: {fleet_result['output_path']}")
            
            if results.get('consolidated', {}).get('consolidated_path'):
                print(f"  Consolidated: {results['consolidated']['consolidated_path']}")

            analysis_summary = results.get("consolidated", {}).get("analysis_summary")
            if analysis_summary:
                print("\nAnalysis Summary:")
                print(f"  Rows: {analysis_summary.get('row_count', 0)}")
                print(f"  Columns: {analysis_summary.get('column_count', 0)}")

                fleet_counts = analysis_summary.get("fleet_counts")
                if fleet_counts:
                    print("  Fleet counts:")
                    for fleet, count in fleet_counts.items():
                        print(f"    {fleet}: {count}")

                numeric_summary = analysis_summary.get("numeric_summary")
                if numeric_summary:
                    print("  Numeric summary (count/mean/min/max):")
                    for column, stats in numeric_summary.items():
                        stats_line = ", ".join(f"{stat}={value}" for stat, value in stats.items())
                        print(f"    {column}: {stats_line}")

            export_choice = input("\nExport consolidated CSV? (y/N): ").strip().lower()
            if export_choice in {"y", "yes"}:
                try:
                    csv_path = pipeline.export_consolidated_csv()
                    print(f"CSV exported to: {csv_path}")
                except Exception as e:
                    logger.error(f"CSV export failed: {str(e)}")
                    print(f"CSV export failed: {str(e)}")
        
        else:
            print(f"Error: {results.get('error', 'Unknown error')}")
        
        print("="*60)
        
        return results['status'] == 'completed'
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        print(f"\nPipeline failed: {str(e)}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
