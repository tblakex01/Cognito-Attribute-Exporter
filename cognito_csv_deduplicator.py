#!/usr/bin/env python3
"""
Cognito CSV Deduplicator

This script removes duplicate entries from a CSV file exported from AWS Cognito User Pool.
It identifies duplicates based on configurable key fields (e.g., username, email, sub).
"""

import argparse
import csv
import logging
import os
import sys
from typing import List, Dict, Set, Any, Optional


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)


class CsvDeduplicator:
    """Handles deduplication of CSV files based on specified key columns."""
    
    def __init__(
        self,
        input_file: str,
        output_file: Optional[str] = None,
        key_fields: List[str] = None,
        keep_first: bool = True,
        dry_run: bool = False
    ):
        """
        Initialize the CSV deduplicator.
        
        Args:
            input_file: Path to the input CSV file
            output_file: Path to the output CSV file (defaults to input file with _deduplicated suffix)
            key_fields: List of column names to use as unique keys (defaults to ['sub'])
            keep_first: Whether to keep the first occurrence of a duplicate (True) or the last (False)
            dry_run: Only report duplicates without modifying files
        """
        self.input_file = input_file
        
        # Default output file name if not provided
        if output_file is None:
            base, ext = os.path.splitext(input_file)
            self.output_file = f"{base}_deduplicated{ext}"
        else:
            self.output_file = output_file
        
        # Default key fields if not provided
        self.key_fields = key_fields or ['sub']
        self.keep_first = keep_first
        self.dry_run = dry_run
        
        # Stats
        self.total_rows = 0
        self.duplicate_count = 0
        self.unique_rows = 0

    def validate_key_fields(self, header: List[str]) -> bool:
        """
        Validate that all key fields exist in the CSV header.
        
        Args:
            header: List of column names from the CSV
            
        Returns:
            True if all key fields exist, False otherwise
        """
        missing_fields = [field for field in self.key_fields if field not in header]
        
        if missing_fields:
            logger.error(f"Key fields not found in CSV: {', '.join(missing_fields)}")
            logger.error(f"Available fields: {', '.join(header)}")
            return False
            
        return True

    def get_row_key(self, row: Dict[str, str]) -> str:
        """
        Create a unique key for a row based on the key fields.
        
        Args:
            row: Dictionary representing a CSV row
            
        Returns:
            String key representing the unique identifier for this row
        """
        # Create a tuple of values for the key fields
        key_values = tuple(row.get(field, '') for field in self.key_fields)
        return str(key_values)

    def deduplicate(self) -> bool:
        """
        Deduplicate the CSV file.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if input file exists
            if not os.path.exists(self.input_file):
                logger.error(f"Input file does not exist: {self.input_file}")
                return False
                
            # Read the input file to count total rows and check column names
            with open(self.input_file, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if not header:
                    logger.error("CSV file is empty or has no header row")
                    return False
                
                # Count total rows
                self.total_rows = sum(1 for _ in reader)
                
            # Re-open the file for actual processing
            with open(self.input_file, 'r', newline='', encoding='utf-8') as in_file:
                reader = csv.DictReader(in_file)
                
                # Validate key fields exist in the CSV
                if not self.validate_key_fields(reader.fieldnames):
                    return False
                
                # Set to track seen keys
                seen_keys: Set[str] = set()
                duplicates: Set[str] = set()
                
                # For dry run or keep_last mode, we need to store all rows
                all_rows = []
                
                # Process all rows
                for row in reader:
                    row_key = self.get_row_key(row)
                    
                    # Store the row for later processing
                    all_rows.append((row_key, row))
                    
                    # Track duplicates
                    if row_key in seen_keys:
                        duplicates.add(row_key)
                    else:
                        seen_keys.add(row_key)
                
                self.duplicate_count = len(duplicates)
                self.unique_rows = len(seen_keys)
                
                # Report findings
                logger.info(f"Total rows: {self.total_rows}")
                logger.info(f"Unique entities: {self.unique_rows}")
                logger.info(f"Duplicate entries found: {self.duplicate_count}")