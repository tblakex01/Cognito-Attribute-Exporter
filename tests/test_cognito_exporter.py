import unittest
from unittest.mock import patch, MagicMock
import os
import gzip
import shutil
import sys
from io import StringIO
from botocore.exceptions import ClientError

from cognito_attribute_exporter.cognito_exporter import CognitoExporter, parse_arguments

# Helper to bypass the retry decorator for specific tests
def mock_with_backoff_retry_passthrough(func, *args, **kwargs):
    return func(*args, **kwargs)

class TestCognitoExporterS3Upload(unittest.TestCase):

    def setUp(self):
        self.test_output_filename = "test_output.csv"
        self.test_output_gz_filename = f"{self.test_output_filename}.gz"

        # Create a dummy output file
        with open(self.test_output_filename, "w") as f:
            f.write("col1,col2\nval1,val2\n")

        # Instantiate CognitoExporter with minimal args
        # We'll mock the boto3 clients directly on the instance after creation
        with patch('boto3.client'), patch('boto3.Session'):
            self.exporter = CognitoExporter(
                user_pool_id="us-east-1_testpool",
                region="us-east-1",
                output_file=self.test_output_filename
            )

        # Replace the actual s3_client with a mock
        self.exporter.s3_client = MagicMock()
        # It's good practice to also mock the other client if its methods might be unexpectedly called
        self.exporter.client = MagicMock()


    def tearDown(self):
        # Clean up created files
        if os.path.exists(self.test_output_filename):
            os.remove(self.test_output_filename)
        if os.path.exists(self.test_output_gz_filename):
            os.remove(self.test_output_gz_filename)

    @patch.object(CognitoExporter, 'with_backoff_retry', side_effect=mock_with_backoff_retry_passthrough)
    def test_upload_to_s3_uncompressed(self, mock_retry):
        bucket_name = "test-bucket"
        s3_key = "uncompressed_test.csv"

        self.exporter.upload_to_s3(bucket=bucket_name, key=s3_key, compress=False)

        self.exporter.s3_client.upload_file.assert_called_once_with(
            self.test_output_filename,
            bucket_name,
            s3_key
        )
        self.assertFalse(os.path.exists(self.test_output_gz_filename), "Compressed file should not exist")

    @patch.object(CognitoExporter, 'with_backoff_retry', side_effect=mock_with_backoff_retry_passthrough)
    def test_upload_to_s3_compressed_success(self, mock_retry):
        bucket_name = "test-bucket"
        s3_key = "compressed_test.csv.gz"

        self.exporter.upload_to_s3(bucket=bucket_name, key=s3_key, compress=True)

        self.exporter.s3_client.upload_file.assert_called_once_with(
            self.test_output_gz_filename, # Expect .gz file
            bucket_name,
            s3_key
        )
        self.assertFalse(os.path.exists(self.test_output_gz_filename), "Compressed .gz file should be removed after successful upload")

    @patch.object(CognitoExporter, 'with_backoff_retry', side_effect=mock_with_backoff_retry_passthrough)
    def test_upload_to_s3_compressed_s3_error_cleanup(self, mock_retry):
        bucket_name = "test-bucket"
        s3_key = "error_test.csv.gz"

        # Configure the mock s3_client's upload_file method to raise ClientError
        # The error response structure needs to be similar to what botocore would provide
        mock_error_response = {'Error': {'Code': 'SomeS3Error', 'Message': 'Mock S3 error'}}
        self.exporter.s3_client.upload_file.side_effect = ClientError(mock_error_response, "upload_file")

        with self.assertRaises(ClientError) as context:
            self.exporter.upload_to_s3(bucket=bucket_name, key=s3_key, compress=True)

        self.assertIn("Mock S3 error", str(context.exception))

        self.exporter.s3_client.upload_file.assert_called_once_with(
            self.test_output_gz_filename,
            bucket_name,
            s3_key
        )
        self.assertFalse(os.path.exists(self.test_output_gz_filename), "Compressed .gz file should be removed even if S3 upload fails")

    @patch.object(CognitoExporter, 'with_backoff_retry', side_effect=mock_with_backoff_retry_passthrough)
    def test_upload_to_s3_compressed_key_is_none(self, mock_retry):
        bucket_name = "test-bucket"
        # When key is None, it should use the basename of the (compressed) file
        expected_s3_key = os.path.basename(self.test_output_gz_filename)

        self.exporter.upload_to_s3(bucket=bucket_name, key=None, compress=True)

        self.exporter.s3_client.upload_file.assert_called_once_with(
            self.test_output_gz_filename,
            bucket_name,
            expected_s3_key
        )
        self.assertFalse(os.path.exists(self.test_output_gz_filename))

    @patch.object(CognitoExporter, 'with_backoff_retry', side_effect=mock_with_backoff_retry_passthrough)
    def test_upload_to_s3_uncompressed_key_is_none(self, mock_retry):
        bucket_name = "test-bucket"
        # When key is None, it should use the basename of the original file
        expected_s3_key = os.path.basename(self.test_output_filename)

        self.exporter.upload_to_s3(bucket=bucket_name, key=None, compress=False)

        self.exporter.s3_client.upload_file.assert_called_once_with(
            self.test_output_filename,
            bucket_name,
            expected_s3_key
        )
        self.assertFalse(os.path.exists(self.test_output_gz_filename))


class TestArgumentParsing(unittest.TestCase):
    """Test argument parsing, especially mutual exclusivity constraints."""

    def test_filter_expression_and_group_name_mutual_exclusivity(self):
        """Test that --filter-expression and --group-name cannot be used together."""
        # Save original sys.argv
        original_argv = sys.argv

        try:
            # Attempt to use both --filter-expression and --group-name
            sys.argv = [
                'test',
                '--user-pool-id', 'us-east-1_test',
                '--export-all',
                '--filter-expression', 'email ^= "test"',
                '--group-name', 'TestGroup'
            ]

            # Capture stderr to check for the mutual exclusivity error
            with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
                with self.assertRaises(SystemExit) as cm:
                    parse_arguments()

                # Check that it exited with error code
                self.assertNotEqual(cm.exception.code, 0)

                # Check that the error message mentions the mutual exclusivity
                stderr_output = mock_stderr.getvalue()
                self.assertIn('not allowed with argument', stderr_output.lower())

        finally:
            # Restore original sys.argv
            sys.argv = original_argv

    def test_filter_expression_alone_is_valid(self):
        """Test that --filter-expression can be used alone."""
        original_argv = sys.argv

        try:
            sys.argv = [
                'test',
                '--user-pool-id', 'us-east-1_test',
                '--export-all',
                '--filter-expression', 'email ^= "test"'
            ]

            # This should not raise an exception
            args = parse_arguments()
            self.assertEqual(args.filter_expression, 'email ^= "test"')
            self.assertIsNone(args.group_name)

        finally:
            sys.argv = original_argv

    def test_group_name_alone_is_valid(self):
        """Test that --group-name can be used alone."""
        original_argv = sys.argv

        try:
            sys.argv = [
                'test',
                '--user-pool-id', 'us-east-1_test',
                '--export-all',
                '--group-name', 'TestGroup'
            ]

            # This should not raise an exception
            args = parse_arguments()
            self.assertIsNone(args.filter_expression)
            self.assertEqual(args.group_name, 'TestGroup')

        finally:
            sys.argv = original_argv


if __name__ == '__main__':
    unittest.main()
