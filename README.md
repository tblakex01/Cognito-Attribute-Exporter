# 🔐 Cognito Attribute Exporter

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/downloads/)
[![AWS](https://img.shields.io/badge/AWS-Cognito-orange)](https://aws.amazon.com/cognito/)
[![Code Style](https://img.shields.io/badge/code%20style-PEP8-green)](https://www.python.org/dev/peps/pep-0008/)

A comprehensive toolkit for exporting and managing user data from AWS Cognito User Pools.

## ✨ Features

- 📤 **Robust Cognito Data Export**: Export user attributes from AWS Cognito User Pools to CSV format
- 🔄 **Exponential Backoff with Jitter**: Automatically handles AWS API rate limits with intelligent retry logic
- ⏸️ **Checkpoint & Resume**: Save progress during exports and resume from where you left off
- 🧹 **CSV Deduplication**: Remove duplicate user entries from exported CSV files
- 🔍 **Flexible Attribute Selection**: Export specific attributes or discover and export all available attributes
- 📃 **Pagination Support**: Efficiently handles large user pools with proper pagination

## 🚀 Installation

### Prerequisites

- Python 3.10+
- AWS credentials configured (either via environment variables, credentials file, or IAM role)
- Poetry for dependency management.

### Setup

1. Clone this repository:
   ```bash
   git clone https://github.com/tblakex01/Cognito-Attribute-Exporter.git
   cd Cognito-Attribute-Exporter
   ```

2. Install dependencies using Poetry:
   ```bash
   poetry install
   ```
   Poetry will handle the creation of a virtual environment and installation of all necessary dependencies.

## 🧪 Running Tests

This project uses Pytest for testing. To run the tests:

1.  Ensure you have installed the development dependencies:
    ```bash
    poetry install --with dev
    ```
2.  Run Pytest:
    ```bash
    poetry run pytest
    ```

## 📖 Usage

### Cognito User Pool Export

The main export tool supports various options for exporting user data from Cognito:

```bash
python cognito_exporter.py --user-pool-id YOUR_POOL_ID --export-all
```

#### Key Parameters:

- `--user-pool-id`: Your Cognito User Pool ID (required)
- `--export-all`: Export all available attributes
- `-attr, --export-attributes`: List specific attributes to export
- `--region`: AWS region (default: us-east-1)
- `--profile`: AWS profile to use
- `-f, --file-name`: Output CSV filename
- `--max-retries`: Maximum retry attempts for rate-limited requests
- `--resume`: Resume from last saved checkpoint

#### Example Commands:

📋 Export all attributes:
```bash
python cognito_exporter.py --user-pool-id us-east-1_abcdefghi --export-all
```

📋 Export specific attributes:
```bash
python cognito_exporter.py --user-pool-id us-east-1_abcdefghi --export-attributes username email phone_number
```

📋 Resume an interrupted export:
```bash
python cognito_exporter.py --user-pool-id us-east-1_abcdefghi --export-all --resume
```

📋 Custom retry settings:
```bash
python cognito_exporter.py --user-pool-id us-east-1_abcdefghi --export-all --max-retries 10 --base-delay 1.0
```

### CSV Deduplication

The deduplication tool helps remove duplicate entries from exported CSV files:

```bash
python cognito_csv_deduplicator.py CognitoUsers.csv
```

#### Key Parameters:

- `input_file`: Path to the CSV file to deduplicate (required)
- `-o, --output-file`: Custom output file path
- `-k, --keys`: Column names to use as unique keys (default: sub)
- `--keep-last`: Keep the last occurrence of duplicates instead of the first
- `--dry-run`: Check for duplicates without modifying files

#### Example Commands:

📝 Basic deduplication:
```bash
python cognito_csv_deduplicator.py CognitoUsers.csv
```

📝 Custom key fields:
```bash
python cognito_csv_deduplicator.py CognitoUsers.csv -k username email
```

📝 Check for duplicates without making changes:
```bash
python cognito_csv_deduplicator.py CognitoUsers.csv --dry-run
```

## ⚡ Handling AWS API Rate Limits

The Cognito Exporter includes built-in features to handle AWS API rate limits:

1. 📈 **Exponential Backoff**: Automatically increases wait time between retries
2. 🎲 **Jitter**: Adds randomness to retry intervals to prevent synchronized retries
3. ⚙️ **Configurable Retry Parameters**: Customize max retries and delay settings
4. 🛡️ **Built-in Rate Limiting**: Adds small delays between API calls to reduce throttling

## 🔧 Advanced Features

### 🔖 Checkpointing

The export process automatically saves checkpoints to allow resuming interrupted exports:

- Checkpoints are saved every 10 pages or 500 records
- Use the `--resume` flag to continue from the last checkpoint
- Checkpoint files are saved with the `.checkpoint` extension

### 🔍 Attribute Discovery

When using `--export-all`, the tool automatically:

1. Samples users to discover all available attributes
2. Includes both standard and custom attributes
3. Falls back to common attributes if no users are found

## ❓ Troubleshooting

- **Rate Limiting Errors**: Try increasing `--base-delay` and `--max-retries`
- **Memory Issues**: Export specific attributes instead of all attributes
- **CSV Parsing Problems**: Ensure the CSV is properly encoded (UTF-8)

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

---

<p align="center">
  Made with ❤️ for AWS Cognito users by Anthony Michaels.
</p>
