"""
Logging utilities for the data ingestion pipeline.
"""
import logging
import sys
from pathlib import Path
from typing import Optional

from config import LOGGING_CONFIG


def setup_logger(name: str, log_file: Optional[Path] = None) -> logging.Logger:
    """
    Set up a logger with console and file handlers.
    
    Args:
        name: Logger name
        log_file: Optional log file path
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, LOGGING_CONFIG["level"]))
    
    # Clear existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(LOGGING_CONFIG["format"])
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler
    file_path = log_file or LOGGING_CONFIG["file"]
    file_path.parent.mkdir(exist_ok=True)
    file_handler = logging.FileHandler(file_path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance."""
    return logging.getLogger(name)
