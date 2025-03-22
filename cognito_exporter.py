#!/usr/bin/env python3
"""
Cognito User Pool Export Tool

This script exports user records from an AWS Cognito User Pool to a CSV file.
It supports filtering by specific attributes and pagination for large user pools.
Features exponential backoff and retry mechanism to handle rate limiting.
"""

import boto3
import csv
import sys
import time
import random
import logging
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from botocore.exceptions import ClientError
from typing import Dict, List, Optional, Any, Tuple


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('cognito_export.log')
    ]
)
logger = logging.getLogger(__name__)


class CognitoExporter:
    """Handles exporting users from a Cognito User Pool to CSV with retry mechanism."""
    
    # Common attributes found in Cognito User Pools
    COMMON_ATTRIBUTES = [
        'sub', 'username', 'email', 'email_verified', 'phone_number', 'phone_number_verified',
        'name', 'given_name', 'family_name', 'middle_name', 'nickname', 'preferred_username',
        'profile', 'picture', 'website', 'gender', 'birthdate', 'zoneinfo', 'locale', 'address',
        'updated_at', 'cognito:mfa_enabled', 'cognito:username', 'cognito:roles', 'cognito:groups',
        'custom:tenant_id', 'UserCreateDate', 'UserLastModifiedDate', 'Enabled', 'UserStatus'
    ]
    
    # Define retry configuration
    MAX_RETRIES = 8
    BASE_DELAY = 0.5  # Base delay in seconds
    MAX_DELAY = 30.0  # Maximum delay in seconds
    JITTER = 0.25  # Jitter factor for randomization
    
    def __init__(
        self, 
        user_pool_id: str, 
        region: str, 
        attributes: List[str] = None,
        export_all: bool = False,
        profile: str = None, 
        output_file: str = 'CognitoUsers.csv',
        max_records: int = 0,
        page_size: int = 60,
        starting_token: str = None,
        max_retries: int = None,
        base_delay: float = None
    ):
        """
        Initialize the Cognito Exporter with the specified parameters.
        
        Args:
            user_pool_id: The Cognito User Pool ID
            region: AWS region where the pool is located
            attributes: List of user attributes to export
            export_all: Whether to export all available attributes
            profile: AWS profile to use (optional)
            output_file: Path to CSV output file
            max_records: Maximum number of records to export (0 for all)
            page_size: Number of records per page (max 60)
            starting_token: Token to resume pagination from a previous run
            max_retries: Maximum number of retry attempts (defaults to class constant)
            base_delay: Base delay for exponential backoff (defaults to class constant)
        """
        self.user_pool_id = user_pool_id
        self.region = region
        self.export_all = export_all
        self.output_file = output_file
        self.max_records = max_records
        self.page_size = min(page_size, 60)  # Cognito API limit is 60
        self.pagination_token = starting_token
        
        # Retry configuration
        self.max_retries = max_retries if max_retries is not None else self.MAX_RETRIES
        self.base_delay = base_delay if base_delay is not None else self.BASE_DELAY
        
        # Initialize AWS client
        if profile:
            session = boto3.Session(profile_name=profile)
            self.client = session.client('cognito-idp', region)
        else:
            self.client = boto3.client('cognito-idp', region)
            
        # Set up attributes to export
        self.attributes = attributes
        
        # If export_all is requested, we need to determine all available attributes
        if export_all:
            self.attributes = self.discover_all_attributes()
    
    def with_backoff_retry(self, func, *args, **kwargs):
        """
        Execute a function with exponential backoff and retry.
        
        Args:
            func: Function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function
            
        Returns:
            Result from the function
            
        Raises:
            Exception: If the maximum number of retries is exceeded
        """
        retries = 0
        while True:
            try:
                return func(*args, **kwargs)
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', '')
                error_message = e.response.get('Error', {}).get('Message', '')
                
                # Check if this is a throttling error
                if error_code in ('ThrottlingException', 'TooManyRequestsException', 
                                 'Throttling', 'LimitExceededException'):
                    if retries >= self.max_retries:
                        logger.error(f"Maximum retries exceeded: {error_message}")
                        raise
                    
                    # Calculate delay with exponential backoff and jitter
                    delay = min(
                        self.MAX_DELAY,
                        self.base_delay * (2 ** retries)
                    )
                    jitter = random.uniform(-self.JITTER * delay, self.JITTER * delay)
                    delay = max(0, delay + jitter)
                    
                    retries += 1
                    logger.warning(
                        f"Rate limit error: {error_code}. Retrying in {delay:.2f}s "
                        f"(Attempt {retries}/{self.max_retries})"
                    )
                    time.sleep(delay)
                else:
                    # For non-throttling errors, don't retry
                    logger.error(f"AWS error: {error_code} - {error_message}")
                    raise
            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")
                raise