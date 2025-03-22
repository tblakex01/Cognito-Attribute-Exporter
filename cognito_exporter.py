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