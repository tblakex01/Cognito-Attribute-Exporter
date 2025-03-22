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