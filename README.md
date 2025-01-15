# DevRev Contact Merge Tool

A robust Python-based tool designed to safely merge duplicate contacts in the DevRev system, with a special focus on preserving ticket data and conversations. This tool specifically handles migrations between REVU- and user_ format contacts while maintaining data integrity and providing comprehensive backup capabilities.

## ⚠️ Important Safety Notes

- **ALWAYS RUN IN PREVIEW MODE FIRST**: Never execute merges in production without first using the `--preview` flag
- **START WITH A SMALL BATCH**: Test with a limited dataset using the test sample creation utility
- **BACKUP VERIFICATION IS CRITICAL**: The tool will abort if backup verification fails - this is a safety feature, do not bypass
- **NETWORK STABILITY**: Ensure stable network connection during the merge process to prevent interrupted operations
- **DISK SPACE**: Ensure sufficient space for backups (approximately 1MB per ticket with conversations)

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Testing](#testing)
- [Usage](#usage)
- [Backup Structure](#backup-structure)
- [Error Handling](#error-handling)
- [Monitoring and Maintenance](#monitoring-and-maintenance)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)
- [Support](#support)

## Features

### Core Functionality
- **Smart Duplicate Detection:**
  - Identifies duplicate contacts based on email address
  - Matches REVU- format with corresponding user_ format contacts
  - Verifies match criteria before proceeding
  - Validates data consistency before merge

- **Automated Merge Process:**
  - Preserves primary contact's display name and metadata
  - Transfers tickets and conversations without data loss
  - Updates external references automatically
  - Handles merge conflicts gracefully
  - Maintains audit trail of all operations

### Data Protection
- **Comprehensive Backup System:**
  - Creates timestamped backups before each merge
  - Stores complete ticket history and conversations
  - Maintains file attachments and metadata
  - Organizes backups by contact and date
  - Implements verification checksums

- **Integrity Verification:**
  - Validates backup completeness
  - Verifies ticket counts match expected totals
  - Checks conversation threads integrity
  - Confirms successful merges
  - Ensures data consistency post-merge

### Error Handling & Recovery
- **Robust API Communication:**
  - Implements rate limiting (45 calls per minute)
  - Uses exponential backoff for retries (up to 3 attempts)
  - Handles network timeouts gracefully
  - Maintains session persistence
  - Provides detailed error reporting

- **Savepoint System:**
  - Tracks successfully processed merges
  - Enables resume after interruption
  - Prevents duplicate processing
  - Maintains audit trail
  - Supports rollback capabilities

### Monitoring & Reporting
- **Detailed Logging:**
  - Records all operations in timestamped logs
  - Captures API responses and errors
  - Tracks backup operations
  - Logs verification results
  - Provides operation timestamps

- **Comprehensive Reporting:**
  - Generates detailed merge reports in JSON format
  - Summarizes successful and failed operations
  - Provides backup location references
  - Includes ticket migration statistics
  - Tracks performance metrics

  ## Prerequisites

- Python 3.8 or higher
- Required Python Packages:
  ```
  requests==2.31.0
  python-dotenv==1.0.0
  ratelimit==2.2.1
  ```
- DevRev API access with appropriate permissions
- Stable network connection
- Sufficient disk space for backups (estimate ~1MB per ticket with conversations)
- Access to Snowflake for generating the contact CSV

## Setup

### Environment Setup
1. **Clone the Repository:**
   ```bash
   git clone https://github.com/velocityglobal/devrev-contact-merge-tool.git
   cd devrev-contact-merge-tool
   ```

2. **Create Virtual Environment:**
   ```bash
   python -m venv venv
   
   # On Windows:
   .\venv\Scripts\activate
   
   # On macOS/Linux:
   source venv/bin/activate
   ```

3. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

### Configuration
1. **Environment Variables:**
   Create a `.env` file in the project root:
   ```env
   # Required
   DEVREV_API_TOKEN=your_api_token_here
   
   # Optional
   DEVREV_BASE_URL=https://api.devrev.ai
   RATE_LIMIT_CALLS=45
   RATE_LIMIT_PERIOD=60
   ```

2. **Directory Structure Setup:**
   ```bash
   # Create required directories
   mkdir -p logs reports backups
   
   # Set appropriate permissions
   chmod 755 logs reports backups
   ```

### API Access Setup
1. **Generate API Token:**
   - Log in to DevRev admin panel
   - Navigate to API Settings
   - Generate a new API token
   - Ensure token has required permissions:
     - Read/Write access to contacts
     - Read access to tickets
     - Read access to conversations

2. **Verify API Access:**
   ```bash
   python merge_contacts.py --test-api
   ```

### CSV File Preparation
Generate the input CSV using this Snowflake query:
```sql
WITH ticket_counts AS (
    SELECT 
        r.REV_USER_ID,
        GREATEST(
            COUNT(DISTINCT CASE WHEN w.OWNED_BY_ID = r.REV_USER_ID THEN w.WORK_ID END),
            COUNT(DISTINCT CASE WHEN w.CREATED_BY_ID = r.REV_USER_ID THEN w.WORK_ID END),
            COUNT(DISTINCT CASE WHEN w.REPORTED_BY_ID = r.REV_USER_ID THEN w.WORK_ID END)
        ) as total_tickets
    FROM ANALYTICS.DBT_BASE_DEVREV.DEVREV_REV_USERS r
    LEFT JOIN ANALYTICS.DBT_BASE_DEVREV.DEVREV_WORKS w 
        ON (w.OWNED_BY_ID = r.REV_USER_ID 
            OR w.CREATED_BY_ID = r.REV_USER_ID
            OR w.REPORTED_BY_ID = r.REV_USER_ID)
    WHERE w.TYPE IN ('ticket', 'issue')
    GROUP BY r.REV_USER_ID
)
SELECT 
    r.REV_USER_ID,
    r.DISPLAY_NAME,
    r.EMAIL,
    r.EXTERNAL_REF,
    r.FULL_NAME,
    r.CREATED_AT,
    r.UPDATED_AT,
    COALESCE(t.total_tickets, 0) as TICKET_COUNT
FROM ANALYTICS.DBT_BASE_DEVREV.DEVREV_REV_USERS r
LEFT JOIN ticket_counts t ON r.REV_USER_ID = t.REV_USER_ID
WHERE r.EMAIL ILIKE '%@velocityglobal.com%'
AND (
    r.EXTERNAL_REF LIKE 'REVU-%'
    OR 
    r.EXTERNAL_REF LIKE 'user_%'
)
ORDER BY r.EMAIL;

## Testing

### Creating a Test Sample
Before running on all contacts, create a small test sample:
```bash
# Create test sample with 4 contacts
python merge_contacts.py --create-test-sample \
  --input contacts.csv \
  --output test_contacts.csv \
  --size 4
```

### Validating Backups
After running the tool, verify your backups:
```bash
# Check backup integrity
python verify_backups.py --backup-dir backups/
```

## Usage

### Before Running
1. Ensure sufficient disk space for backups
2. Verify API token permissions
3. Test network connectivity to DevRev API
4. Create a backup of your CSV file

### Running the Tool

1. **Preview Mode (Required first step)**:
   ```bash
   python merge_contacts.py --csv contacts.csv --preview
   ```

2. **Test Sample (Recommended second step)**:
   ```bash
   python merge_contacts.py --csv test_contacts.csv
   ```

3. **Full Run**:
   ```bash
   python merge_contacts.py --csv contacts.csv
   ```

## Backup Structure

```
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
```

## Error Handling

### Common Issues and Solutions

#### API Rate Limiting
- Tool automatically handles rate limiting
- Implements exponential backoff
- Maximum 3 retry attempts
- 60-second cooldown period

#### Network Timeouts
- Automatic retry mechanism
- Connection timeout: 30 seconds
- Read timeout: 60 seconds
- Maximum 3 retry attempts

#### Verification Failures
- Check logs for specific mismatch details
- Verify backup data integrity
- Confirm ticket count accuracy
- Validate conversation threads

#### Incomplete Merges
- Use savepoints to resume operations
- Check partial backup integrity
- Verify merge state
- Review error logs

### Recovery Process
If a merge fails:
1. Check the logs for the specific error
2. Verify the backup data is intact
3. Use the backup verification tool
4. Resume from last savepoint if needed

## Monitoring and Maintenance

### Log Monitoring
Monitor these directories:
- `logs/` - Detailed operation logs
- `reports/` - Merge summaries
- `backups/` - Backup integrity reports

## Best Practices

### Before Running
- Verify DevRev API access
- Check disk space for backups
- Test with a small sample
- Run in preview mode
- Review current contact data

### During Execution
- Monitor log files
- Watch for error patterns
- Keep terminal window open
- Monitor system resources
- Check network stability

### After Completion
- Verify merge report
- Check backup integrity
- Archive important logs
- Clean up temporary files
- Document any issues

## Troubleshooting

### Common Issues

#### API Authentication Failures
```
Error: 401 Unauthorized
```
- Verify API token is valid
- Check token permissions
- Ensure token isn't expired
- Confirm environment variables

#### Network Timeout Errors
```
Error: Connection timed out
```
- Check network connectivity
- Verify API endpoint status
- Confirm firewall settings
- Test with smaller batch size

#### Backup Verification Errors
```
Error: Ticket count mismatch
```
- Compare backup data with source
- Check disk space
- Verify file permissions
- Review corrupted backups

#### Merge Conflicts
```
Error: Conflict detected
```
- Review contact states
- Check external references
- Verify ticket associations
- Examine error logs

## Contributing

### Submitting Changes
1. Fork the repository
2. Create a new branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run the test suite
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

### Development Guidelines
- Follow PEP 8 style guidelines
- Add unit tests for new features
- Update documentation as needed
- Keep commits atomic and well-described
- Test thoroughly with preview mode

### Bug Reports
- Use the GitHub issue tracker
- Include Python and dependency versions
- Provide sample data (sanitized if needed)
- Describe expected vs actual behavior

## License

MIT License

Copyright (c) 2024 Velocity Global

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

## Support

### Getting Help
1. **Check Existing Resources:**
   - Read the troubleshooting section
   - Search closed issues
   - Review the logs

2. **Gather Information:**
   - Script version
   - Full error message and stack trace
   - Relevant log files
   - Sample CSV data (sanitized)
   - Steps to reproduce

3. **Opening an Issue:**
   Create a new issue on GitHub including:
   - Detailed description
   - Steps to reproduce
   - Expected vs actual behavior
   - Log files
   - Environment details

### Quick Support Checklist
- [ ] Using latest version
- [ ] API token has correct permissions
- [ ] Run in preview mode first
- [ ] Checked logs for errors
- [ ] Tried suggested solutions

### Contact
For urgent issues or security concerns:
- Engineering Team: engineering@velocityglobal.com
- DevOps Support: devops@velocityglobal.com

