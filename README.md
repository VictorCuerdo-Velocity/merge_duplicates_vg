# DevRev Contact Merge Tool

This repository contains a Python-based tool designed to merge duplicate contacts in the DevRev system, specifically handling the migration of tickets and conversations between REVU- and user_ format contacts while maintaining data integrity.

## ⚠️ Important Safety Notes

- **ALWAYS RUN IN PREVIEW MODE FIRST**: Before executing any merges in production, use the `--preview` flag to verify the intended changes.
- **START WITH A SMALL BATCH**: Test with a limited dataset first using the test sample creation utility.
- **BACKUP VERIFICATION IS CRITICAL**: The tool will abort if backup verification fails - do not bypass this check.
- **NETWORK CONNECTIVITY**: Ensure stable network connection during the merge process to prevent interrupted operations.

## Features

[Previous features section remains the same...]

## Prerequisites

- Python 3.8+
- Required Python Packages:
  - `requests`
  - `python-dotenv`
  - `ratelimit`
- Stable network connection
- DevRev API access with appropriate permissions
- Sufficient disk space for backups (estimate ~1MB per ticket with conversations)

## Setup

[Previous setup section remains...]

## Testing

### Creating a Test Sample
```python
# Create a small test sample before running on all contacts
python create_test_sample.py --input contacts.csv --output test_contacts.csv --size 4

Validating Backups
After running the tool, verify your backups:

# Check backup integrity
python verify_backups.py --backup-dir backups/

Usage
Before Running

1. Ensure sufficient disk space for backups
2. Verify API token permissions
3. Test network connectivity to DevRev API
4. Create a backup of your CSV file

Running the Tool

1. Preview Mode (Required first step):
python merge_contacts.py --csv contacts.csv --preview

2. Test Sample (Recommended second step):
python merge_contacts.py --csv test_contacts.csv

3. Full Run:
python merge_contacts.py --csv contacts.csv

Backup Structure
backups/
├── user@velocityglobal.com_20250115_123456/
│   ├── tickets.json           # All tickets data
│   ├── metadata.json         # Contact metadata
│   ├── conversations/        # Conversations directory
│   │   ├── ticket1/         # Per-ticket conversations
│   │   │   ├── messages.json
│   │   │   └── attachments/
│   │   └── ticket2/
│   └── verification.json    # Backup integrity data

Error Handling
Common Issues and Solutions

* API Rate Limiting: The tool automatically handles rate limiting with exponential backoff
* Network Timeouts: Automatic retry mechanism for transient failures
* Verification Failures: Check logs for specific mismatch details
* Incomplete Merges: Use savepoints to resume interrupted operations

Recovery Process
If a merge fails:

1. Check the logs for the specific error
2. Verify the backup data is intact
3. Use the backup verification tool
4. Resume from last savepoint if needed

Monitoring and Maintenance
Log Monitoring

* Check logs/ directory for detailed operation logs
* Monitor the reports/ directory for merge summaries
* Review backup integrity reports

Cleanup

* Regular cleanup of old backup data (After verifying successful merges)
* Archive completed merge reports
* Maintain savepoint files for reference

Best Practices

1. Before Running:

* Verify DevRev API access
* Check disk space for backups
* Test with a small sample
* Run in preview mode


2. During Execution:

* Monitor log files
* Watch for error patterns
* Keep terminal window open


3. After Completion:

* Verify merge report
* Check backup integrity
* Archive important logs
* Clean up temporary files

Troubleshooting
Common issues and their solutions:

* API Authentication failures
* Network timeout handling
* Backup verification errors
* Merge conflict resolution

## Contributing

We welcome contributions to improve the DevRev Contact Merge Tool! Here's how you can help:

### Submitting Changes
1. Fork the repository
2. Create a new branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run the test suite to ensure everything works
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

### Development Guidelines
- Follow PEP 8 style guidelines
- Add unit tests for new features
- Update documentation as needed
- Keep commits atomic and well-described
- Test thoroughly with preview mode before submitting

### Bug Reports
- Use the GitHub issue tracker
- Include Python and dependency versions
- Provide sample data (sanitized if needed)
- Describe expected vs actual behavior

## License

This project is licensed under the MIT License - see below for details:


