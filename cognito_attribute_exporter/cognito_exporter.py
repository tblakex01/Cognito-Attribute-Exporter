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
import os
import gzip
import shutil
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from botocore.exceptions import ClientError
from typing import Dict, List, Optional, Any, Tuple


# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", handlers=[logging.StreamHandler(), logging.FileHandler("cognito_export.log")])
logger = logging.getLogger(__name__)


class CognitoExporter:
    """Handles exporting users from a Cognito User Pool to CSV with retry mechanism."""

    # Common attributes found in Cognito User Pools
    COMMON_ATTRIBUTES = [
        "sub",
        "username",
        "email",
        "email_verified",
        "phone_number",
        "phone_number_verified",
        "name",
        "given_name",
        "family_name",
        "middle_name",
        "nickname",
        "preferred_username",
        "profile",
        "picture",
        "website",
        "gender",
        "birthdate",
        "zoneinfo",
        "locale",
        "address",
        "updated_at",
        "cognito:mfa_enabled",
        "cognito:username",
        "cognito:roles",
        "cognito:groups",
        "custom:tenant_id",
        "UserCreateDate",
        "UserLastModifiedDate",
        "Enabled",
        "UserStatus",
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
        output_file: str = "CognitoUsers.csv",
        max_records: int = 0,
        page_size: int = 60,
        starting_token: str = None,
        max_retries: int = None,
        base_delay: float = None,
        filter_expression: str = None,
        group_name: str = None,
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
            filter_expression: Optional filter expression for Cognito list_users
            group_name: Optional Cognito group name to export users from
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

        # Initialize AWS clients
        if profile:
            session = boto3.Session(profile_name=profile)
            self.client = session.client("cognito-idp", region)
            self.s3_client = session.client("s3", region)
        else:
            self.client = boto3.client("cognito-idp", region)
            self.s3_client = boto3.client("s3", region)

        # Set up attributes to export
        self.attributes = attributes

        # Filtering/group options
        self.filter_expression = filter_expression
        self.group_name = group_name

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
                error_code = e.response.get("Error", {}).get("Code", "")
                error_message = e.response.get("Error", {}).get("Message", "")

                # Check if this is a throttling error
                if error_code in ("ThrottlingException", "TooManyRequestsException", "Throttling", "LimitExceededException"):
                    if retries >= self.max_retries:
                        logger.error(f"Maximum retries exceeded: {error_message}")
                        raise

                    # Calculate delay with exponential backoff and jitter
                    delay = min(self.MAX_DELAY, self.base_delay * (2**retries))
                    jitter = random.uniform(-self.JITTER * delay, self.JITTER * delay)
                    delay = max(0, delay + jitter)

                    retries += 1
                    logger.warning(f"Rate limit error: {error_code}. Retrying in {delay:.2f}s (Attempt {retries}/{self.max_retries})")
                    time.sleep(delay)
                else:
                    # For non-throttling errors, don't retry
                    logger.error(f"AWS error: {error_code} - {error_message}")
                    raise
            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")
                raise

    def discover_all_attributes(self) -> List[str]:
        """
        Discovers all available attributes in the user pool by sampling users.

        Returns:
            List of attribute names found in the user pool
        """
        try:
            # Try to get a sample of users to discover attributes
            response = self.with_backoff_retry(self.get_users_without_retry)

            if not response.get("Users"):
                logger.info("No users found in the pool. Using common attributes list.")
                return self.COMMON_ATTRIBUTES

            # Initialize with common attribute names
            all_attributes = set(self.COMMON_ATTRIBUTES)

            # Process the first few users to gather all possible attributes
            for user in response.get("Users", [])[:5]:  # Sample up to 5 users
                # Add root level attributes
                all_attributes.update(user.keys())

                # Add attributes from the Attributes list
                for attr in user.get("Attributes", []):
                    all_attributes.add(attr["Name"])

            # Convert to list and sort for consistent output
            return sorted(list(all_attributes))

        except Exception as e:
            logger.error(f"Error discovering attributes: {str(e)}")
            logger.info("Falling back to common attributes list.")
            return self.COMMON_ATTRIBUTES

    def get_users_without_retry(self, pagination_token: Optional[str] = None) -> Dict[str, Any]:
        """
        Raw method to fetch users from Cognito without retry logic.

        Args:
            pagination_token: Token for pagination

        Returns:
            Response from Cognito API
        """
        if self.group_name:
            params = {
                "UserPoolId": self.user_pool_id,
                "GroupName": self.group_name,
                "Limit": self.page_size,
            }
            if pagination_token:
                params["NextToken"] = pagination_token
            return self.client.list_users_in_group(**params)
        else:
            params = {"UserPoolId": self.user_pool_id, "Limit": self.page_size}
            if pagination_token:
                params["PaginationToken"] = pagination_token
            if self.filter_expression:
                params["Filter"] = self.filter_expression
            return self.client.list_users(**params)

    def get_users(self, pagination_token: Optional[str] = None) -> Dict[str, Any]:
        """
        Fetch a page of users from the Cognito User Pool with retry logic.

        Args:
            pagination_token: Token for pagination

        Returns:
            Response from Cognito API with users and pagination token
        """
        return self.with_backoff_retry(self.get_users_without_retry, pagination_token)

    def extract_user_attributes(self, user: Dict[str, Any]) -> Dict[str, str]:
        """
        Extract the requested attributes from a user record.

        Args:
            user: User record from Cognito

        Returns:
            Dictionary with requested attributes
        """
        result = {attr: "" for attr in self.attributes}

        # Extract attributes that are at the user root level
        for attr in self.attributes:
            if attr in user:
                # Handle complex objects by converting to JSON if needed
                if isinstance(user[attr], (dict, list)):
                    import json
                    result[attr] = json.dumps(user[attr])
                else:
                    # Always use string representation
                    result[attr] = str(user[attr])

        # Extract attributes from the Attributes list
        for attr_obj in user.get("Attributes", []):
            attr_name = attr_obj["Name"]
            if attr_name in self.attributes:
                # Ensure all values are stored as strings
                result[attr_name] = str(attr_obj["Value"])

        return result

    def save_checkpoint(self, total_exported: int) -> None:
        """
        Save checkpoint information to allow resuming the export.

        Args:
            total_exported: Number of records exported so far
        """
        checkpoint_data = {"pagination_token": self.pagination_token, "total_exported": total_exported, "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")}

        import json

        with open(f"{self.output_file}.checkpoint", "w") as f:
            json.dump(checkpoint_data, f)

        logger.info(f"Checkpoint saved: {total_exported} records exported, token: {self.pagination_token[:10]}...")

    def upload_to_s3(self, bucket: str, key: Optional[str] = None, compress: bool = False) -> None:
        """Upload the exported CSV to S3, optionally compressing it first."""
        file_to_upload = self.output_file
        upload_key = key or os.path.basename(self.output_file)
        gz_path = None  # Initialize gz_path

        try:
            if compress:
                gz_path = f"{self.output_file}.gz"
                with open(self.output_file, "rb") as src, gzip.open(gz_path, "wb") as dst:
                    shutil.copyfileobj(src, dst)
                file_to_upload = gz_path
                upload_key = key or os.path.basename(gz_path)

            self.with_backoff_retry(self.s3_client.upload_file, file_to_upload, bucket, upload_key)
            logger.info(f"Uploaded {file_to_upload} to s3://{bucket}/{upload_key}")

        finally:
            if compress and gz_path and os.path.exists(gz_path):
                os.remove(gz_path)

    def export_users(self) -> int:
        """
        Export users from the Cognito User Pool to a CSV file.

        Returns:
            Number of exported records
        """
        try:
            # Print attribute information
            logger.info(f"Exporting {len(self.attributes)} attributes:")
            logger.info(f"  {', '.join(self.attributes[:5])}{'...' if len(self.attributes) > 5 else ''}")

            # Open output file with context manager for safe handling
            with open(self.output_file, "w", newline="", encoding="utf-8") as csv_file:
                # Create CSV writer and write header
                # Set quoting to ensure all fields are quoted to preserve string nature
                writer = csv.DictWriter(
                    csv_file, 
                    fieldnames=self.attributes,
                    quoting=csv.QUOTE_ALL  # Force quotes around all fields
                )
                writer.writeheader()

                page_count = 0
                total_exported = 0
                last_checkpoint = 0

                # Loop through all pages of users
                while True:
                    try:
                        response = self.get_users(self.pagination_token)
                    except Exception as e:
                        logger.error(f"Error fetching users: {str(e)}")
                        # Save checkpoint before exiting
                        if self.pagination_token:
                            self.save_checkpoint(total_exported)
                        return total_exported

                    # Process users in this page
                    users_in_page = response.get("Users", [])
                    if not users_in_page:
                        logger.warning("No users returned in this page. This may indicate an issue.")

                    for user in users_in_page:
                        user_data = self.extract_user_attributes(user)
                        writer.writerow(user_data)
                        total_exported += 1

                        # Check if we've reached the maximum records limit
                        if self.max_records and total_exported >= self.max_records:
                            logger.info(f"Maximum records limit ({self.max_records}) reached.")
                            return total_exported

                    # Update pagination info
                    page_count += 1
                    logger.info(f"Processed page: {page_count} | Total exported records: {total_exported}")

                    # Save checkpoint periodically (every 10 pages or 500 records)
                    if (total_exported - last_checkpoint >= 500) or (page_count % 10 == 0):
                        if self.pagination_token:
                            self.save_checkpoint(total_exported)
                            last_checkpoint = total_exported

                    # Check for more pages
                    self.pagination_token = response.get("PaginationToken") or response.get("NextToken")
                    if not self.pagination_token:
                        logger.info("End of Cognito User Pool reached.")
                        break

                    # Built-in rate limiting to reduce chances of hitting limits
                    time.sleep(0.2)

                return total_exported

        except IOError as e:
            logger.error(f"Error creating/writing to file {self.output_file}: {str(e)}")
            return 0
        except KeyboardInterrupt:
            logger.info("Operation interrupted by user.")
            # Save checkpoint before exiting on Ctrl+C
            if self.pagination_token:
                self.save_checkpoint(total_exported)
            return total_exported


def parse_arguments():
    """Parse command line arguments."""
    parser = ArgumentParser(description="Export Cognito User Pool records to CSV file with retry mechanism", formatter_class=ArgumentDefaultsHelpFormatter)

    attr_group = parser.add_mutually_exclusive_group(required=True)
    attr_group.add_argument("-attr", "--export-attributes", nargs="+", type=str, help="List of attributes to be saved in CSV")
    attr_group.add_argument("--export-all", action="store_true", help="Export all available user attributes")

    parser.add_argument("--user-pool-id", type=str, help="The user pool ID", required=True)

    parser.add_argument("--region", type=str, default="us-east-1", help="The user pool region")

    parser.add_argument("--profile", type=str, default=None, help="The AWS profile to use")

    # Create mutually exclusive group for filtering options
    filter_group = parser.add_mutually_exclusive_group()
    filter_group.add_argument("--filter-expression", type=str, default=None, help="Filter expression for list_users (cannot be used with --group-name)")
    filter_group.add_argument("--group-name", type=str, default=None, help="Name of Cognito group to export (cannot be used with --filter-expression)")

    parser.add_argument("--starting-token", type=str, default=None, help="Starting pagination token (for resuming interrupted exports)")

    parser.add_argument("-f", "--file-name", type=str, default="CognitoUsers.csv", help="CSV File name")

    parser.add_argument("--page-size", type=int, default=60, help="Number of records per page (max 60)")

    parser.add_argument("--num-records", type=int, default=0, help="Max number of Cognito records to export (0 for all)")

    parser.add_argument("--max-retries", type=int, default=CognitoExporter.MAX_RETRIES, help="Maximum number of retry attempts for rate-limited requests")

    parser.add_argument("--base-delay", type=float, default=CognitoExporter.BASE_DELAY, help="Base delay in seconds for exponential backoff")

    parser.add_argument("--s3-bucket", type=str, default=None, help="S3 bucket to upload the CSV")
    parser.add_argument("--s3-key", type=str, default=None, help="S3 object key for upload (defaults to filename)")
    parser.add_argument("--compress", action="store_true", help="Compress CSV before uploading to S3")

    parser.add_argument("--resume", action="store_true", help="Resume export from the last saved checkpoint")

    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], default="INFO", help="Set the logging level")

    return parser.parse_args()  # No string literal or comment here


def load_checkpoint(filename: str) -> Tuple[str, int]:
    """
    Load checkpoint data from a previous run.

    Args:
        filename: Base filename of the export

    Returns:
        Tuple containing (pagination_token, total_exported)
    """
    import json

    try:
        with open(f"{filename}.checkpoint", "r") as f:
            data = json.load(f)

        token = data.get("pagination_token")
        total = data.get("total_exported", 0)
        timestamp = data.get("timestamp", "unknown time")

        logger.info(f"Resuming export from checkpoint: {total} records exported at {timestamp}")
        return token, total
    except (IOError, json.JSONDecodeError) as e:
        logger.error(f"Failed to load checkpoint: {str(e)}")
        return None, 0


def main():
    """Main entry point for the script."""
    args = parse_arguments()

    # Set logging level based on argument
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    try:
        starting_token = args.starting_token

        # If resuming from checkpoint, load the pagination token
        if args.resume and not starting_token:
            checkpoint_token, _ = load_checkpoint(args.file_name)
            if checkpoint_token:
                starting_token = checkpoint_token
            else:
                logger.warning("No valid checkpoint found. Starting from beginning.")

        # Create and run the exporter
        exporter = CognitoExporter(
            user_pool_id=args.user_pool_id,
            region=args.region,
            attributes=args.export_attributes if hasattr(args, "export_attributes") and args.export_attributes else None,
            export_all=args.export_all if hasattr(args, "export_all") else False,
            profile=args.profile,
            output_file=args.file_name,
            max_records=args.num_records,
            page_size=args.page_size,
            starting_token=starting_token,
            max_retries=args.max_retries,
            base_delay=args.base_delay,
            filter_expression=args.filter_expression,
            group_name=args.group_name,
        )

        start_time = time.time()
        total_exported = exporter.export_users()
        end_time = time.time()

        duration = end_time - start_time
        logger.info(f"\nExport completed successfully.")
        logger.info(f"Total records: {total_exported}")
        logger.info(f"Output file: {args.file_name}")
        logger.info(f"Duration: {duration:.2f} seconds")

        if total_exported > 0:
            logger.info(f"Average time per record: {(duration / total_exported):.4f} seconds")

        if args.s3_bucket:
            try:
                exporter.upload_to_s3(args.s3_bucket, args.s3_key, compress=args.compress)
            except Exception as e:
                logger.error(f"Failed to upload to S3: {str(e)}")

        return 0

    except KeyboardInterrupt:
        logger.info("Operation interrupted by user.")
        return 130  # Standard exit code for Ctrl+C
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
