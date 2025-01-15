#!/usr/bin/env python3

import csv
import os
import json
import time
import argparse
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Set
import requests
from dotenv import load_dotenv
from ratelimit import limits, sleep_and_retry
from dataclasses import dataclass
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

# Global variable to store the current log file's path
LOG_FILE_PATH = None

# Load environment variables
load_dotenv()

# Configure logging
def setup_logging():
    """Configure logging with both file and console output"""
    global LOG_FILE_PATH
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    logfile = log_dir / f"contact_merge_{timestamp}.log"
    LOG_FILE_PATH = logfile

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(logfile),
            logging.StreamHandler(),
        ],
    )
    return logging.getLogger(__name__)

logger = setup_logging()

@dataclass
class Contact:
    """Data class to represent a contact with validation"""

    rev_user_id: str
    display_name: str
    email: str
    external_ref: str
    full_name: str
    ticket_count: int
    created_at: str
    updated_at: str

    @classmethod
    def from_dict(cls, data: Dict) -> "Contact":
        """Create a Contact instance from a dictionary with validation"""
        required_fields = {
            "REV_USER_ID",
            "DISPLAY_NAME",
            "EMAIL",
            "EXTERNAL_REF",
            "FULL_NAME",
            "TICKET_COUNT",
            "CREATED_AT",
            "UPDATED_AT",
        }
        missing_fields = required_fields - set(data.keys())
        if missing_fields:
            raise ValueError(f"Missing required fields: {missing_fields}")

        return cls(
            rev_user_id=str(data["REV_USER_ID"]),
            display_name=str(data["DISPLAY_NAME"]),
            email=str(data["EMAIL"]),
            external_ref=str(data["EXTERNAL_REF"]),
            full_name=str(data["FULL_NAME"]),
            ticket_count=int(data.get("TICKET_COUNT", 0)),
            created_at=str(data["CREATED_AT"]),
            updated_at=str(data["UPDATED_AT"]),
        )

    def is_revu_contact(self) -> bool:
        """Check if this is a REVU- format contact"""
        return self.external_ref and self.external_ref.startswith("REVU-")

    def is_user_contact(self) -> bool:
        """Check if this is a user_ format contact"""
        return self.external_ref and self.external_ref.startswith("user_")


class RetryableError(Exception):
    """Exception class for errors that should trigger a retry"""
    pass


class DevRevAPI:
    def __init__(self, api_token: str, base_url: str = "https://api.devrev.ai"):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {api_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )
        self.max_retries = 3
        self.retry_delay = 2  # seconds
        self.rate_limit_calls = 45
        self.rate_limit_period = 60

    @sleep_and_retry
    @limits(calls=45, period=60)  # Conservative rate limit
    def make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        retry_count: int = 0,
    ) -> requests.Response:
        """Make a rate-limited API request with retries"""
        url = f"{self.base_url}{endpoint}"

        try:
            response = self.session.request(method, url, json=data)

            if response.status_code in (429, 500, 502, 503, 504):
                raise RetryableError(f"Retryable status code: {response.status_code}")

            response.raise_for_status()
            return response

        except (RetryableError, requests.exceptions.RequestException) as e:
            if retry_count < self.max_retries:
                sleep_time = self.retry_delay * (2**retry_count)  # Exponential backoff
                logger.warning(f"Request failed, retrying in {sleep_time}s: {str(e)}")
                time.sleep(sleep_time)
                return self.make_request(method, endpoint, data, retry_count + 1)
            raise

    def merge_contacts(self, primary_id: str, secondary_id: str) -> bool:
        """Merge two contacts using the DevRev merge endpoint"""
        endpoint = "/api/gateway/internal/rev-users.merge"
        payload = {"primary_user": primary_id, "secondary_user": secondary_id}

        try:
            self.make_request("POST", endpoint, payload)
            logger.info(f"Successfully merged {secondary_id} into {primary_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to merge contacts: {str(e)}")
            return False

    def verify_merge(self, secondary_id: str) -> bool:
        """Verify the secondary contact was properly merged/deleted"""
        endpoint = f"/rev-users.get?id={secondary_id}"
        try:
            response = self.make_request("GET", endpoint)
            # If we can still get the contact, merge wasn't successful
            return False
        except requests.exceptions.RequestException as e:
            if e.response and e.response.status_code == 404:
                return True
            raise

    def update_external_ref(self, contact_id: str, external_ref: str) -> bool:
        """Update external_ref of a contact"""
        endpoint = "/rev-users.update"
        payload = {
            "id": contact_id,
            "external_ref": external_ref,
            "custom_schema_spec": {
                "tenant_fragment": True,
                "validate_required_fields": True,
            },
        }

        try:
            self.make_request("POST", endpoint, payload)
            logger.info(f"Successfully updated external_ref for {contact_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to update external_ref: {str(e)}")
            return False

    def backup_contact_data(self, contact: Contact) -> bool:
        """Back up all contact data including tickets and conversations"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = Path(f"backups/{contact.email}_{timestamp}")
        backup_dir.mkdir(parents=True, exist_ok=True)

        try:
            # Get all tickets for this contact
            endpoint = "/works.list"
            payload = {"type": ["ticket", "issue"], "owned_by": [contact.rev_user_id]}
            tickets_response = self.make_request("POST", endpoint, payload).json()

            # Save tickets data
            with open(backup_dir / "tickets.json", "w", encoding="utf-8") as f:
                json.dump(tickets_response, f, indent=2)

            # For each ticket, get conversations
            for ticket in tickets_response.get("works", []):
                ticket_id = ticket["id"]
                conv_endpoint = "/conversations.list"
                conv_payload = {"work": ticket_id}
                conv_response = self.make_request("POST", conv_endpoint, conv_payload).json()

                # Save conversations data
                with open(
                    backup_dir / f"conversations_{ticket_id}.json",
                    "w",
                    encoding="utf-8",
                ) as f:
                    json.dump(conv_response, f, indent=2)

            logger.info(f"✓ Backed up data for contact {contact.email} to {backup_dir}")
            return True

        except Exception as e:
            logger.error(f"Failed to backup contact data for {contact.email}: {str(e)}")
            return False


class SavePoint:
    """Class to manage merge operation savepoints"""

    def __init__(self, path: str = "savepoint.json"):
        self.path = Path(path)
        self.processed_pairs: Set[Tuple[str, str]] = set()
        self.load()

    def load(self) -> None:
        """Load existing savepoint if it exists"""
        if self.path.exists():
            try:
                with open(self.path) as f:
                    data = json.load(f)
                    self.processed_pairs = set(tuple(p) for p in data["processed_pairs"])
                logger.info(f"Loaded savepoint with {len(self.processed_pairs)} processed pairs")
            except Exception as e:
                logger.error(f"Error loading savepoint: {e}")

    def save(self) -> None:
        """Save current state to savepoint file"""
        try:
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "processed_pairs": [list(p) for p in self.processed_pairs],
                        "last_updated": datetime.now().isoformat(),
                    },
                    f,
                    indent=2,
                )
        except Exception as e:
            logger.error(f"Error saving savepoint: {e}")

    def add_processed_pair(self, primary_id: str, duplicate_id: str) -> None:
        """Record a successfully processed pair"""
        self.processed_pairs.add((primary_id, duplicate_id))
        self.save()

    def is_processed(self, primary_id: str, duplicate_id: str) -> bool:
        """Check if a pair has already been processed"""
        return (primary_id, duplicate_id) in self.processed_pairs


class ContactMerger:
    def __init__(self, api: DevRevAPI):
        self.api = api
        self.savepoint = SavePoint()
        self.merged_pairs: List[Tuple[Contact, Contact]] = []
        self.failed_merges: List[Tuple[Contact, Contact, str]] = []
        self.preview_mode = False

    def identify_duplicates(self, contacts: List[Contact]) -> List[Tuple[Contact, Contact]]:
        """
        Identify valid duplicate pairs based on our specific criteria:
        - Same email
        - One contact has a REVU- external_ref
        - One contact has a user_ external_ref
        """
        contact_groups: Dict[str, List[Contact]] = {}

        # Group contacts by email
        for contact in contacts:
            key = contact.email.lower()
            contact_groups.setdefault(key, []).append(contact)

        # Debug: Log email groups (optional)
        for email, group in contact_groups.items():
            logger.info(f"Email: {email} has {len(group)} record(s)")

        duplicates = []
        for email, group in contact_groups.items():
            # Only process if there are duplicates
            if len(group) < 2:
                continue

            revu_contact = None
            user_contact = None

            for contact in group:
                if contact.is_revu_contact():
                    revu_contact = contact
                elif contact.is_user_contact():
                    user_contact = contact

            if revu_contact and user_contact:
                if not self.savepoint.is_processed(revu_contact.rev_user_id, user_contact.rev_user_id):
                    duplicates.append((revu_contact, user_contact))
            else:
                # This group does not contain both types of external references.
                # Log detailed information including all external_ref values.
                external_refs = [contact.external_ref for contact in group]
                logger.info(f"Skipped duplicate group for email '{email}'. External references: {external_refs}")

        return duplicates

    def verify_backup_integrity(self, primary: Contact, duplicate: Contact) -> bool:
        """Verify backup data matches actual ticket counts"""
        backup_dir = Path("backups")
        try:
            # Identify the latest backup for the primary contact
            primary_backups = list(backup_dir.glob(f"{primary.email}_*"))
            if not primary_backups:
                logger.error("No backup found for primary contact")
                return False
            latest_primary = max(primary_backups, key=lambda p: p.stat().st_mtime)

            # Identify the latest backup for the duplicate contact
            duplicate_backups = list(backup_dir.glob(f"{duplicate.email}_*"))
            if not duplicate_backups:
                logger.error("No backup found for duplicate contact")
                return False
            latest_duplicate = max(duplicate_backups, key=lambda p: p.stat().st_mtime)

            # Check primary contact tickets
            with open(latest_primary / "tickets.json", encoding="utf-8") as f:
                primary_tickets = json.load(f)
                if len(primary_tickets.get("works", [])) != primary.ticket_count:
                    logger.error(
                        f"Primary contact ticket count mismatch! Expected {primary.ticket_count} but found {len(primary_tickets.get('works', []))}"
                    )
                    return False

            # Check duplicate contact tickets
            with open(latest_duplicate / "tickets.json", encoding="utf-8") as f:
                duplicate_tickets = json.load(f)
                if len(duplicate_tickets.get("works", [])) != duplicate.ticket_count:
                    logger.error(
                        f"Duplicate contact ticket count mismatch! Expected {duplicate.ticket_count} but found {len(duplicate_tickets.get('works', []))}"
                    )
                    return False

            logger.info("Backup integrity verification passed.")
            return True

        except Exception as e:
            logger.error(f"Backup verification failed: {str(e)}")
            return False

    def merge_contacts(self, primary: Contact, duplicate: Contact) -> bool:
        """
        Merge duplicate contact into primary contact with safety checks.
        """
        try:
            logger.info(f"\nMerging contacts for email {primary.email}")
            logger.info(f"Primary: {primary.display_name} ({primary.rev_user_id})")
            logger.info(f"Duplicate: {duplicate.display_name} ({duplicate.rev_user_id})")

            if self.preview_mode:
                logger.info("PREVIEW MODE - Would make these changes:")
                logger.info("1. Backup all data for both contacts")
                logger.info(f"2. Merge {duplicate.rev_user_id} into {primary.rev_user_id}")
                logger.info(f"3. Update primary contact's external_ref to: {duplicate.external_ref}")
                return True

            # Step 1: Backup both contacts' data
            logger.info("Backing up contact data...")
            if not self.api.backup_contact_data(primary):
                raise Exception("Failed to backup primary contact data - aborting merge")
            if not self.api.backup_contact_data(duplicate):
                raise Exception("Failed to backup duplicate contact data - aborting merge")

            # Step 2: Verify ticket counts match backup
            if not self.verify_backup_integrity(primary, duplicate):
                raise Exception("Backup verification failed - aborting merge")

            # Step 3: Merge contacts via API
            if not self.api.merge_contacts(primary.rev_user_id, duplicate.rev_user_id):
                raise Exception("Failed to merge contacts")

            # Small delay to ensure merge is processed
            time.sleep(1)
            if not self.api.verify_merge(duplicate.rev_user_id):
                raise Exception("Merge verification failed")

            # Step 4: Update primary's external_ref
            if not self.api.update_external_ref(primary.rev_user_id, duplicate.external_ref):
                raise Exception("Failed to update external_ref")

            self.merged_pairs.append((primary, duplicate))
            self.savepoint.add_processed_pair(primary.rev_user_id, duplicate.rev_user_id)
            logger.info("✓ Merge completed successfully")
            return True

        except Exception as e:
            self.failed_merges.append((primary, duplicate, str(e)))
            logger.error(f"✗ Failed to merge contacts: {str(e)}")
            return False

    def generate_html_report(self) -> None:
        """Generate an HTML report of the merge operations"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_dir = Path("reports")
        report_dir.mkdir(exist_ok=True)

        # Create the Jinja2 environment and load the template
        env = Environment(loader=FileSystemLoader('templates'))
        template = env.get_template('report_template.html')

        # Read the log file contents (if available)
        log_content = ""
        if LOG_FILE_PATH and LOG_FILE_PATH.exists():
            try:
                with open(LOG_FILE_PATH, "r", encoding="utf-8") as f:
                    log_content = f.read()
            except Exception as e:
                logger.error(f"Failed to read log file: {e}")

        # Prepare the data for the template
        report_data = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "summary": {
                "total_merges_attempted": len(self.merged_pairs) + len(self.failed_merges),
                "successful_merges": len(self.merged_pairs),
                "failed_merges": len(self.failed_merges),
            },
            "successful_merges": [
                {
                    "primary": {
                        "display_name": primary.display_name,
                        "email": primary.email,
                        "original_external_ref": primary.external_ref,
                        "original_ticket_count": primary.ticket_count,
                        "final_ticket_count": primary.ticket_count + duplicate.ticket_count,
                    },
                    "duplicate": {
                        "display_name": duplicate.display_name,
                        "email": duplicate.email,
                        "external_ref": duplicate.external_ref,
                        "ticket_count": duplicate.ticket_count,
                    },
                }
                for primary, duplicate in self.merged_pairs
            ],
            "failed_merges": [
                {
                    "primary": {
                        "display_name": primary.display_name,
                        "email": primary.email,
                    },
                    "duplicate": {
                        "display_name": duplicate.display_name,
                        "email": duplicate.email,
                    },
                    "error": error,
                }
                for primary, duplicate, error in self.failed_merges
            ],
            "log_content": log_content,
        }

        # Render the HTML content using the template
        html_content = template.render(**report_data)

        # Save the HTML file in the reports directory
        report_path = report_dir / f"merge_report_{timestamp}.html"
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(html_content)

        logger.info(f"Generated HTML report: {report_path}")

    def generate_report(self) -> None:
        """Generate a detailed report of the merge operations"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_dir = Path("reports")
        report_dir.mkdir(exist_ok=True)

        report = {
            "summary": {
                "total_merges_attempted": len(self.merged_pairs) + len(self.failed_merges),
                "successful_merges": len(self.merged_pairs),
                "failed_merges": len(self.failed_merges),
            },
            "successful_merges": [
                {
                    "primary": {
                        "display_name": primary.display_name,
                        "email": primary.email,
                        "id": primary.rev_user_id,
                        "original_external_ref": primary.external_ref,
                        "new_external_ref": duplicate.external_ref,
                        "original_ticket_count": primary.ticket_count,
                        "final_ticket_count": primary.ticket_count + duplicate.ticket_count,
                    },
                    "duplicate": {
                        "display_name": duplicate.display_name,
                        "email": duplicate.email,
                        "id": duplicate.rev_user_id,
                        "external_ref": duplicate.external_ref,
                        "ticket_count": duplicate.ticket_count,
                    },
                }
                for primary, duplicate in self.merged_pairs
            ],
            "failed_merges": [
                {
                    "primary": {
                        "display_name": primary.display_name,
                        "email": primary.email,
                        "id": primary.rev_user_id,
                    },
                    "duplicate": {
                        "display_name": duplicate.display_name,
                        "email": duplicate.email,
                        "id": duplicate.rev_user_id,
                    },
                    "error": error,
                }
                for primary, duplicate, error in self.failed_merges
            ],
        }

        report_path = report_dir / f"merge_report_{timestamp}.json"
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        logger.info(f"Generated JSON report: {report_path}")

        # Generate the HTML report as well
        self.generate_html_report()

    def process_csv(self, csv_path: str, preview: bool = False) -> None:
        """Process the CSV file and merge duplicate contacts"""
        self.preview_mode = preview
        contacts = []

        # Read and validate contacts from CSV
        with open(csv_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    contact = Contact.from_dict(row)
                    contacts.append(contact)
                except ValueError as e:
                    logger.error(f"Invalid contact data: {e}")
                    continue

        # Identify duplicates
        duplicates = self.identify_duplicates(contacts)
        logger.info(f"\nFound {len(duplicates)} duplicate pairs to process")

        if preview:
            logger.info("\nPREVIEW MODE - No changes will be made")
            for primary, duplicate in duplicates:
                logger.info(f"\nWould merge:")
                logger.info(f"Primary: {primary.display_name} ({primary.email})")
                logger.info(f"Duplicate: {duplicate.display_name} ({duplicate.email})")
            return

        # Process each duplicate pair
        total = len(duplicates)
        for idx, (primary, duplicate) in enumerate(duplicates, 1):
            logger.info(f"\nProcessing pair {idx}/{total}")
            self.merge_contacts(primary, duplicate)


def main():
    parser = argparse.ArgumentParser(description="Merge duplicate DevRev contacts")
    parser.add_argument("--csv", required=True, help="Path to the CSV file containing contacts")
    parser.add_argument("--preview", action="store_true", help="Preview changes without executing them")
    parser.add_argument("--batch-size", type=int, default=10, help="Number of merges to process in one batch")
    args = parser.parse_args()

    api_token = os.getenv("DEVREV_API_TOKEN")
    if not api_token:
        logger.error("DEVREV_API_TOKEN environment variable is required")
        return

    try:
        # Validate CSV file exists
        if not os.path.exists(args.csv):
            logger.error(f"CSV file not found: {args.csv}")
            return

        # Create required directories
        for directory in ["logs", "reports", "backups"]:
            Path(directory).mkdir(exist_ok=True)

        # Initialize API and merger
        api = DevRevAPI(api_token)
        merger = ContactMerger(api)

        # Process the CSV
        logger.info(f"Starting contact merge process...")
        logger.info(f"CSV file: {args.csv}")
        logger.info(f"Preview mode: {args.preview}")

        merger.process_csv(args.csv, args.preview)
        merger.generate_report()

        # Print summary
        if not args.preview:
            logger.info("\nMerge process completed!")
            logger.info(f"Successfully merged: {len(merger.merged_pairs)} pairs")
            logger.info(f"Failed merges: {len(merger.failed_merges)} pairs")
            if merger.failed_merges:
                logger.info("\nFailed merges:")
                for primary, duplicate, error in merger.failed_merges:
                    logger.info(f"- {primary.email}: {error}")

        logger.info("Both JSON and HTML reports have been generated in the 'reports' directory.")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise


if __name__ == "__main__":
    main()
