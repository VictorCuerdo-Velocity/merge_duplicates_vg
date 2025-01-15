# DevRev Contact Merge Tool

This repository contains a Python-based tool designed to merge duplicate contacts in the DevRev system. It not only automates the merge process through the DevRev API but also incorporates robust safety measures to back up all related ticket and conversation data before proceeding with any merge. This ensures that critical data (including tickets with external communications) is preserved and verified before contact data is altered in production.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Usage](#usage)
  - [CSV Input File](#csv-input-file)
  - [Merge Process](#merge-process)
  - [Preview Mode](#preview-mode)
- [Snowflake Query for Generating contacts.csv](#snowflake-query-for-generating-contactscsv)
- [Logging and Reporting](#logging-and-reporting)
- [Contributing](#contributing)
- [License](#license)

## Features

- **Automated Merge Process:**  
  Identify and merge duplicate contacts based on email and external reference (either `REVU-` or `user_` formats).

- **Data Backup:**  
  Before any merge, the tool backs up all ticket data and associated conversations for both primary and duplicate contacts in a timestamped, structured format.

- **Backup Integrity Verification:**  
  Verifies that the ticket counts in the backup match the expected values to ensure backup integrity.

- **Rate Limiting and Retry Handling:**  
  Incorporates rate limiting and retry logic using the `ratelimit` package to handle API errors and improve reliability during transient failures.

- **Detailed Logging and Reporting:**  
  Logs every step of the process, and generates a detailed JSON report summarizing successful merges and any failures.

## Architecture

The repository is composed of several key components:

- **`Contact` Data Class:**  
  Represents contact records parsed from a CSV file, including methods for validation and type checking based on `external_ref`.

- **`DevRevAPI` Class:**  
  Handles communication with the DevRev API, including merging contacts, verifying merges, updating external references, and backing up contact data.

- **`SavePoint` Class:**  
  Manages the savepoint mechanism to track processed merge pairs and avoid duplicate operations.

- **`ContactMerger` Class:**  
  Orchestrates the overall process: reading contacts from the CSV, identifying duplicates, performing backups, verifying integrity, merging contacts, and generating a report.

- **`main()` Function:**  
  Provides the entry point for the command-line interface, accepts arguments such as the CSV file path and a preview mode flag.

## Prerequisites

- **Python 3.8+**
- **Required Python Packages:**  
  - `requests`
  - `python-dotenv`
  - `ratelimit`
  
  You can install the dependencies via:
  
  ```bash
  pip install -r requirements.txt
Environment Variables:
A .env file must be present in the repository root with at least the following variable defined:

dotenv
Copy code
DEVREV_API_TOKEN=<your_devrev_api_token_here>
Setup
Clone the repository:

bash
Copy code
git clone https://github.com/yourusername/devrev-contact-merge-tool.git
cd devrev-contact-merge-tool
Install the dependencies:

bash
Copy code
pip install -r requirements.txt
Create and configure the .env file:

bash
Copy code
cp .env.example .env
# Edit .env with your DEVREV_API_TOKEN value
Usage
CSV Input File
The tool processes a CSV file containing contact information. The CSV file is generated via a Snowflake query detailed below. Each row in the CSV should contain the following fields (all fields are required):

REV_USER_ID
DISPLAY_NAME
EMAIL
EXTERNAL_REF
FULL_NAME
TICKET_COUNT
CREATED_AT
UPDATED_AT
Merge Process
Run the merge process with:

bash
Copy code
python merge_contacts.py --csv path/to/contacts.csv
This command will:

Read and validate the CSV file.
Identify duplicate contacts based on email and external reference criteria.
Back up ticket and conversation data for each contact.
Perform backup integrity checks.
Execute the merge via the DevRev API.
Generate detailed logs and a summary report (reports/merge_report_<timestamp>.json).
Preview Mode
To see what actions would be taken without performing any actual changes, run:

bash
Copy code
python merge_contacts.py --csv path/to/contacts.csv --preview
This mode outputs the planned actions for each duplicate pair without calling the merge or backup endpoints.

Snowflake Query for Generating contacts.csv
Below is the Snowflake SQL query used to generate the contacts.csv file. This query aggregates ticket counts and selects relevant contact data from the DevRev tables:

sql
Copy code
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
    -- Either REVU- format
    r.EXTERNAL_REF LIKE 'REVU-%'
    OR 
    -- Or user_ format
    r.EXTERNAL_REF LIKE 'user_%'
)
ORDER BY r.EMAIL;
Logging and Reporting
Logs:
Logs are stored in the logs/ directory with each run saved in a timestamped file (e.g., contact_merge_20250115_142305.log).

Backup Files:
Data backup (tickets and conversations) files are stored in the backups/ directory, organized by contact email and timestamp.

Report:
A summary report in JSON format is generated post-merge in the reports/ directory, detailing:

Total merges attempted
Successful and failed merge details
