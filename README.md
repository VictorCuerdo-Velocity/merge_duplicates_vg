# merge_duplicates_vg

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
  - Checks for existing relationships

- **Automated Merge Process:**
  - Preserves primary contact's display name and metadata
  - Transfers tickets and conversations without data loss
  - Updates external references automatically
  - Handles merge conflicts gracefully
  - Maintains audit trail of all operations
  - Validates data integrity at each step

### Data Protection
- **Comprehensive Backup System:**
  - Creates timestamped backups before each merge
  - Stores complete ticket history and conversations
  - Maintains file attachments and metadata
  - Organizes backups by contact and date
  - Implements verification checksums
  - Supports backup rotation

- **Integrity Verification:**
  - Validates backup completeness
  - Verifies ticket counts match expected totals
  - Checks conversation threads integrity
  - Confirms successful merges
  - Ensures data consistency post-merge
  - Validates metadata preservation

### Error Handling & Recovery
- **Robust API Communication:**
  - Implements rate limiting (45 calls per minute)
  - Uses exponential backoff for retries (up to 3 attempts)
  - Handles network timeouts gracefully
  - Maintains session persistence
  - Provides detailed error reporting
  - Supports session recovery

- **Savepoint System:**
  - Tracks successfully processed merges
  - Enables resume after interruption
  - Prevents duplicate processing
  - Maintains audit trail
  - Supports rollback capabilities
  - Preserves operation history

### Monitoring & Reporting
- **Detailed Logging:**
  - Records all operations in timestamped logs
  - Captures API responses and errors
  - Tracks backup operations
  - Logs verification results
  - Provides operation timestamps
  - Maintains error context

- **Comprehensive Reporting:**
  - Generates detailed merge reports in JSON format
  - Summarizes successful and failed operations
  - Provides backup location references
  - Includes ticket migration statistics
  - Tracks performance metrics
  - Reports data integrity status

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
- System memory: minimum 4GB RAM
- Storage: minimum 10GB free space

## Setup

### Environment Setup
1. **Clone the Repository:**
   ```bash
   git clone https://github.com/VictorCuerdo-Velocity/merge_duplicates_vg.git
   cd merge_duplicates_vg
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
   
   # Backup Configuration
   BACKUP_RETENTION_DAYS=30
   MAX_BACKUP_SIZE_GB=50
   ENABLE_COMPRESSION=true
   
   # Logging Configuration
   LOG_LEVEL=INFO
   ENABLE_DEBUG_MODE=false
   ```

2. **Directory Structure Setup:**
   ```bash
   # Create required directories
   mkdir -p logs reports backups
   mkdir -p backups/contacts backups/tickets backups/conversations
   
   # Set appropriate permissions
   chmod 755 logs reports backups
   chmod 700 backups/contacts backups/tickets backups/conversations
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
     - Access to metadata
     - Search capabilities

2. **Verify API Access:**
   ```bash
   python merge_contacts.py --test-api
   ```

3. **Validate Permissions:**
   ```bash
   python merge_contacts.py --verify-permissions
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

### Verification Steps
1. **Contact Data Integrity:**
   ```bash
   python verify_backups.py --verify-contacts
   ```

2. **Ticket Data Validation:**
   ```bash
   python verify_backups.py --verify-tickets
   ```

3. **Conversation Thread Integrity:**
   ```bash
   python verify_backups.py --verify-conversations
   ```

## Usage

### Before Running
1. Ensure sufficient disk space for backups
2. Verify API token permissions
3. Test network connectivity to DevRev API
4. Create a backup of your CSV file
5. Validate system requirements
6. Check current running processes
7. Verify backup directory permissions

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

4. **Advanced Options:**
   ```bash
   python merge_contacts.py --csv contacts.csv \
     --batch-size 50 \
     --retry-attempts 5 \
     --backup-compress \
     --verbose
   ```

## Backup Structure

```
backups/
├── user@velocityglobal.com_20250115_123456/
│   ├── metadata/
│   │   ├── contact_info.json     # Contact metadata
│   │   ├── verification.json     # Integrity checksums
│   │   └── merge_state.json      # Merge process state
│   │
│   ├── tickets/
│   │   ├── tickets.json          # All tickets data
│   │   ├── relationships.json    # Ticket relationships
│   │   └── attachments/          # Ticket attachments
│   │
│   ├── conversations/
│   │   ├── ticket1/
│   │   │   ├── messages.json
│   │   │   ├── participants.json
│   │   │   └── attachments/
│   │   └── ticket2/
│   │       ├── messages.json
│   │       └── attachments/
│   │
│   └── logs/
│       ├── merge_operations.log
│       ├── api_calls.log
│       └── errors.log
```

### Backup Contents Description
- **metadata/**: Contains contact information and verification data
- **tickets/**: Stores ticket data and related attachments
- **conversations/**: Maintains conversation threads and messages
- **logs/**: Keeps detailed operation logs

### Backup Verification Process
1. Checksum validation
2. Data completeness check
3. Relationship verification
4. Attachment validation
5. Metadata consistency check

## Error Handling

### Common Issues and Solutions

#### API Rate Limiting
```
Error: 429 Too Many Requests
```
- Tool automatically handles rate limiting
- Implements exponential backoff
- Maximum 3 retry attempts
- 60-second cooldown period
- Batch size adjustment
- Request throttling

#### Network Timeouts
```
Error: Connection timed out after 30000ms
```
- Automatic retry mechanism
- Connection timeout: 30 seconds
- Read timeout: 60 seconds
- Maximum 3 retry attempts
- Session recovery
- State preservation

#### Verification Failures
```
Error: Ticket count mismatch
Expected: 45, Found: 43
```
- Check logs for specific mismatch details
- Verify backup data integrity
- Confirm ticket count accuracy
- Validate conversation threads
- Review metadata consistency
- Check attachment completeness

#### Incomplete Merges
```
Error: Merge operation interrupted
```
- Use savepoints to resume operations
- Check partial backup integrity
- Verify merge state
- Review error logs
- Validate data consistency
- Restore from last known good state

### Recovery Process
If a merge fails:

1. **Initial Assessment:**
   - Check the logs for specific error
   - Identify failure point
   - Review system state
   - Verify data integrity

2. **Backup Verification:**
   - Verify backup data is intact
   - Check file permissions
   - Validate checksums
   - Review backup logs

3. **Recovery Steps:**
   - Use backup verification tool
   - Resume from last savepoint
   - Verify recovered state
   - Validate data consistency

4. **Post-Recovery:**
   - Document incident
   - Update error handling
   - Adjust retry parameters
   - Improve monitoring

## Monitoring and Maintenance

### Log Monitoring
Monitor these directories:
- `logs/` - Detailed operation logs
- `reports/` - Merge summaries
- `backups/` - Backup integrity reports

### Real-time Monitoring
1. **Process Monitoring:**
   - Active merges
   - API call rates
   - Error frequencies
   - System resources

2. **Resource Usage:**
   - Disk space
   - Memory utilization
   - Network bandwidth
   - API quota

3. **Error Tracking:**
   - Failed merges
   - API errors
   - Timeout occurrences
   - Verification failures

### Regular Maintenance
1. **Backup Cleanup:**
   - Remove old backups
   - Compress archived data
   - Verify backup integrity
   - Update backup catalog

2. **Log Rotation:**
   - Archive old logs
   - Compress log files
   - Update log indexes
   - Clean temporary files

3. **Performance Optimization:**
   - Review API usage
   - Adjust batch sizes
   - Update rate limits
   - Optimize storage

   ## Best Practices

### Before Running
- Verify DevRev API access
- Check disk space for backups
- Test with a small sample
- Run in preview mode
- Review current contact data
- Validate system requirements
- Check network connectivity
- Review permission settings

### During Execution
- Monitor log files actively
- Watch for error patterns
- Keep terminal window open
- Monitor system resources
- Check network stability
- Observe API response times
- Track merge progress
- Monitor backup sizes

### After Completion
- Verify merge report
- Check backup integrity
- Archive important logs
- Clean up temporary files
- Document any issues
- Update documentation
- Review error patterns
- Optimize configurations

## Troubleshooting

### Common Issues

#### API Authentication Failures
```
Error: 401 Unauthorized
Solution steps:
1. Verify API token is valid and not expired
2. Check token permissions
3. Ensure environment variables are set correctly
4. Validate API endpoint configuration
```

#### Network Timeout Errors
```
Error: Connection timed out
Resolution steps:
1. Check network connectivity
2. Verify API endpoint status
3. Confirm firewall settings
4. Test with smaller batch size
5. Adjust timeout parameters
```

#### Backup Verification Errors
```
Error: Ticket count mismatch
Troubleshooting:
1. Compare backup data with source
2. Check disk space availability
3. Verify file permissions
4. Review corrupted backups
5. Validate checksum integrity
```

#### Merge Conflicts
```
Error: Conflict detected
Resolution process:
1. Review contact states
2. Check external references
3. Verify ticket associations
4. Examine error logs
5. Validate data consistency
```

## Contributing

### Submitting Changes
1. Fork the repository from [merge_duplicates_vg](https://github.com/VictorCuerdo-Velocity/merge_duplicates_vg)
2. Create a new branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run the test suite
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request to the main repository

### Development Guidelines
- Follow PEP 8 style guidelines
- Add unit tests for new features
- Update documentation as needed
- Keep commits atomic and well-described
- Test thoroughly with preview mode
- Comment code appropriately
- Update CHANGELOG.md
- Maintain type hints

### Bug Reports
- Use the [GitHub issue tracker](https://github.com/VictorCuerdo-Velocity/merge_duplicates_vg/issues)
- Include Python and dependency versions
- Provide sample data (sanitized if needed)
- Describe expected vs actual behavior
- Include relevant log snippets
- Provide environment details
- Document reproduction steps

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
   - Search [closed issues](https://github.com/VictorCuerdo-Velocity/merge_duplicates_vg/issues?q=is%3Aissue+is%3Aclosed)
   - Review the logs
   - Check documentation updates
   - Consult API documentation

2. **Gather Information:**
   - Script version
   - Full error message and stack trace
   - Relevant log files
   - Sample CSV data (sanitized)
   - Steps to reproduce
   - System environment details
   - Network configuration

3. **Opening an Issue:**
   Create a [new issue](https://github.com/VictorCuerdo-Velocity/merge_duplicates_vg/issues/new) including:
   - Detailed description
   - Steps to reproduce
   - Expected vs actual behavior
   - Log files
   - Environment details
   - Screenshots if applicable
   - Relevant configuration

### Quick Support Checklist
- [ ] Using latest version
- [ ] API token has correct permissions
- [ ] Run in preview mode first
- [ ] Checked logs for errors
- [ ] Tried suggested solutions
- [ ] Verified system requirements
- [ ] Checked network connectivity
- [ ] Validated input data
