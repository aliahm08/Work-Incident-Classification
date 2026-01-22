"""
Data source discovery and management.
"""
import fnmatch
from pathlib import Path
from typing import List, Dict, Generator

from config import DATA_SOURCES
from logger import get_logger

logger = get_logger(__name__)


class DataSourceManager:
    """Manages discovery and access to data sources."""
    
    def __init__(self):
        self.sources = DATA_SOURCES
    
    def discover_files(self, source_type: str = "fleet_performance") -> List[Path]:
        """
        Discover all files matching the source type pattern.
        
        Args:
            source_type: Type of data source to discover
            
        Returns:
            List of file paths
        """
        if source_type not in self.sources:
            raise ValueError(f"Unknown source type: {source_type}")
        
        source_config = self.sources[source_type]
        all_files = []
        
        for directory in source_config["directories"]:
            if not directory.exists():
                logger.warning(f"Directory does not exist: {directory}")
                continue
                
            for pattern in source_config["patterns"] if "patterns" in source_config else [source_config["pattern"]]:
                for file_path in directory.rglob(pattern):
                    if self._should_exclude_file(file_path, source_config.get("exclude_patterns", [])):
                        continue
                    all_files.append(file_path)
        
        logger.info(f"Discovered {len(all_files)} files for {source_type}")
        return all_files
    
    def _should_exclude_file(self, file_path: Path, exclude_patterns: List[str]) -> bool:
        """Check if file should be excluded based on patterns."""
        for pattern in exclude_patterns:
            if fnmatch.fnmatch(file_path.name, pattern):
                return True
        return False
    
    def group_files_by_fleet(self, files: List[Path]) -> Dict[str, List[Path]]:
        """
        Group files by fleet number based on directory structure.
        
        Args:
            files: List of file paths
            
        Returns:
            Dictionary mapping fleet names to file lists
        """
        fleet_groups = {}
        
        for file_path in files:
            # Extract fleet info from path
            path_parts = file_path.parts
            fleet_name = None
            
            for part in path_parts:
                if part.lower().startswith("fleet"):
                    fleet_name = part
                    break
            
            if not fleet_name:
                # Use parent directory name as fallback
                fleet_name = file_path.parent.name
            
            if fleet_name not in fleet_groups:
                fleet_groups[fleet_name] = []
            fleet_groups[fleet_name].append(file_path)
        
        return fleet_groups
    
    def get_file_info(self, file_path: Path) -> Dict:
        """
        Get metadata about a file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Dictionary with file metadata
        """
        stat = file_path.stat()
        return {
            "path": str(file_path),
            "name": file_path.name,
            "size_bytes": stat.st_size,
            "modified_time": stat.st_mtime,
            "extension": file_path.suffix.lower()
        }
