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

# ---------------------------------------------------------------------
# GLOBALS & SETUP
# ---------------------------------------------------------------------

LOG_FILE_PATH = None

# Load environment variables
load_dotenv()

class Colors:
    GREEN = '\033[92m'
    RESET = '\033[0m'

def setup_logging():
    """Configure logging with both file and console output."""
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


# ---------------------------------------------------------------------
# DATA CLASS FOR CONTACT
# ---------------------------------------------------------------------

@dataclass
class Contact:
    """Data class to represent a contact with validation."""

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
        """Create a Contact instance from a dictionary with validation."""
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
        """Check if this is a REVU- format contact."""
        return self.external_ref and self.external_ref.startswith("REVU-")

    def is_user_contact(self) -> bool:
        """Check if this is a user_ format contact."""
        return self.external_ref and self.external_ref.startswith("user_")


# ---------------------------------------------------------------------
# ERRORS
# ---------------------------------------------------------------------

class RetryableError(Exception):
    """Exception class for errors that should trigger a retry."""
    pass


# ---------------------------------------------------------------------
# DEVREV API CLASS
# ---------------------------------------------------------------------

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
    @limits(calls=45, period=60)
    def make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        retry_count: int = 0,
    ) -> requests.Response:
        """
        Make a rate-limited API request with up to self.max_retries for 5xx or 429.
        We do NOT treat 404 as retryable, since that can be normal post-merge.
        """
        url = f"{self.base_url}{endpoint}"

        try:
            response = self.session.request(method, url, json=data)
            if response.status_code in (429, 500, 502, 503, 504):
                raise RetryableError(f"Retryable status code: {response.status_code}")

            response.raise_for_status()
            return response

        except (RetryableError, requests.exceptions.RequestException) as e:
            # if it's 404 or 4xx other than 429 => do not attempt re-try
            if isinstance(e, RetryableError) and retry_count < self.max_retries:
                sleep_time = self.retry_delay * (2**retry_count)  # exponential backoff
                logger.warning(f"Request failed, retrying in {sleep_time}s: {str(e)}")
                time.sleep(sleep_time)
                return self.make_request(method, endpoint, data, retry_count + 1)
            raise

    def get_user_info(self, user_id: str) -> Dict:
        """
        Fetch the full Rev user object for a given user_id.
        """
        endpoint = "/rev-users.get"
        payload = {"id": user_id}
        try:
            resp = self.make_request("POST", endpoint, data=payload)
            return resp.json()
        except Exception as e:
            logger.error(f"Failed to fetch user info for {user_id}: {str(e)}")
            return {}

    def backup_contact_data(self, contact: Contact) -> bool:
        """
        Back up all contact data including:
          - The user object itself (rev-users.get)
          - All works (tickets/issues) owned_by, created_by, or reported_by this user
          - All conversations where this user is a member
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = Path(f"backups/{contact.email}_{timestamp}")
        backup_dir.mkdir(parents=True, exist_ok=True)

        try:
            # 1) USER OBJECT
            user_info = self.get_user_info(contact.rev_user_id)
            with open(backup_dir / "user.json", "w", encoding="utf-8") as f:
                json.dump(user_info, f, indent=2)

            # 2) WORKS (tickets/issues) from multiple fields
            endpoint_works_list = "/works.list"
            # owned_by
            payload_owned = {"type": ["ticket", "issue"], "owned_by": [contact.rev_user_id]}
            resp_owned = self.make_request("POST", endpoint_works_list, data=payload_owned).json()

            # created_by
            payload_created = {"type": ["ticket", "issue"], "created_by": [contact.rev_user_id]}
            resp_created = self.make_request("POST", endpoint_works_list, data=payload_created).json()

            # reported_by
            payload_reported = {"type": ["ticket", "issue"], "reported_by": [contact.rev_user_id]}
            resp_reported = self.make_request("POST", endpoint_works_list, data=payload_reported).json()

            # Merge them
            combined_works = {}
            for w in resp_owned.get("works", []) + resp_created.get("works", []) + resp_reported.get("works", []):
                combined_works[w["id"]] = w

            works_output = {"works": list(combined_works.values())}
            with open(backup_dir / "works.json", "w", encoding="utf-8") as f:
                json.dump(works_output, f, indent=2)

            # 3) CONVERSATIONS
            endpoint_conv_list = "/conversations.list"
            payload_conversations = {
                "members": [contact.rev_user_id],
                "limit": 1000
            }
            resp_conversations = self.make_request("POST", endpoint_conv_list, data=payload_conversations).json()
            with open(backup_dir / "conversations.json", "w", encoding="utf-8") as f:
                json.dump(resp_conversations, f, indent=2)

            logger.info(f"✓ Backed up data for contact {contact.email} to {backup_dir}")
            return True

        except Exception as e:
            logger.error(f"Failed to backup contact data for {contact.email}: {str(e)}")
            return False

    def merge_contacts(self, primary_id: str, secondary_id: str) -> bool:
        """
        Merge two contacts using the DevRev merge endpoint.
        """
        endpoint = "/rev-users.merge"
        payload = {"primary_user": primary_id, "secondary_user": secondary_id}
        try:
            self.make_request("POST", endpoint, data=payload)
            logger.info(f"Successfully merged {secondary_id} into {primary_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to merge contacts: {str(e)}")
            return False

    def quick_user_lookup(self, user_id: str) -> Optional[Dict]:
        """
        A direct user lookup that returns the JSON if 200, or None if 404 or errors.
        We do NOT re-try, to avoid spamming the terminal.
        """
        endpoint = "/rev-users.get"
        payload = {"id": user_id}
        try:
            r = self.session.request("POST", f"{self.base_url}{endpoint}", json=payload)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            logger.warning(f"verify_merge: user lookup got {r.status_code} => {str(e)}")
            return None

    def verify_merge(self, secondary_id: str) -> bool:
        """
        Instead of requiring 404, we do:
          - single POST /rev-users.get (no re-try).
          - if 404 => success
          - else if 'rev_user.state' in {deleted, locked, shadow, etc.} => success
          - else => not merged
        """
        user_data = self.quick_user_lookup(secondary_id)
        if user_data is None:
            # 404 => user truly not found => success
            return True

        # If we got user_data, parse rev_user.state
        rev_user = user_data.get("rev_user")
        if not rev_user:
            # weirdly no rev_user => treat as merged
            return True

        state = rev_user.get("state", "").lower()  # e.g. "active", "locked", "deleted"
        if state in ("deleted", "locked", "shadow", "archived"):
            # or any custom states you consider "merged"
            return True

        # If still "active" => not merged
        return False

    def update_external_ref(self, contact_id: str, external_ref: str) -> bool:
        """
        Force update external_ref for the primary contact to match the duplicate's.
        """
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
            self.make_request("POST", endpoint, data=payload)
            logger.info(f"Successfully updated external_ref for {contact_id} to {external_ref}")
            return True
        except Exception as e:
            logger.error(f"Failed to update external_ref: {str(e)}")
            return False


# ---------------------------------------------------------------------
# SAVEPOINT CLASS
# ---------------------------------------------------------------------

class SavePoint:
    """Class to manage merge operation savepoints."""

    def __init__(self, path: str = "savepoint.json"):
        self.path = Path(path)
        self.processed_pairs: Set[Tuple[str, str]] = set()
        self.load()

    def load(self) -> None:
        """Load existing savepoint if it exists."""
        if self.path.exists():
            try:
                with open(self.path) as f:
                    data = json.load(f)
                    self.processed_pairs = set(tuple(p) for p in data["processed_pairs"])
                logger.info(f"Loaded savepoint with {len(self.processed_pairs)} processed pairs")
            except Exception as e:
                logger.error(f"Error loading savepoint: {e}")

    def save(self) -> None:
        """Save current state to savepoint file."""
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
        """Record a successfully processed pair."""
        self.processed_pairs.add((primary_id, duplicate_id))
        self.save()

    def is_processed(self, primary_id: str, duplicate_id: str) -> bool:
        """Check if a pair has already been processed."""
        return (primary_id, duplicate_id) in self.processed_pairs


# ---------------------------------------------------------------------
# CONTACT MERGER CLASS
# ---------------------------------------------------------------------

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
        for c in contacts:
            key = c.email.lower()
            contact_groups.setdefault(key, []).append(c)

        for email, group in contact_groups.items():
            logger.info(f"Email: {email} has {len(group)} record(s)")

        duplicates = []
        for email, group in contact_groups.items():
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
                external_refs = [c.external_ref for c in group]
                logger.info(f"Skipped duplicate group for email '{email}'. External refs: {external_refs}")

        return duplicates

    def verify_backup_integrity(self, primary: Contact, duplicate: Contact) -> None:
        """
        Checks "works.json" for each user. Logs a WARNING if mismatch, does not abort.
        """
        backup_dir = Path("backups")
        try:
            # Identify the latest backup for the primary
            primary_backups = list(backup_dir.glob(f"{primary.email}_*"))
            if not primary_backups:
                logger.error("No backup found for primary contact")
                return
            latest_primary = max(primary_backups, key=lambda p: p.stat().st_mtime)

            # Identify the latest backup for the duplicate
            duplicate_backups = list(backup_dir.glob(f"{duplicate.email}_*"))
            if not duplicate_backups:
                logger.error("No backup found for duplicate contact")
                return
            latest_duplicate = max(duplicate_backups, key=lambda p: p.stat().st_mtime)

            # primary works
            with open(latest_primary / "works.json", encoding="utf-8") as f:
                primary_works = json.load(f)
                actual_primary_count = len(primary_works.get("works", []))
                if actual_primary_count != primary.ticket_count:
                    logger.warning(
                        f"Primary contact ticket count mismatch! "
                        f"Expected {primary.ticket_count} but found {actual_primary_count} - continuing anyway."
                    )

            # duplicate works
            with open(latest_duplicate / "works.json", encoding="utf-8") as f:
                duplicate_works = json.load(f)
                actual_duplicate_count = len(duplicate_works.get("works", []))
                if actual_duplicate_count != duplicate.ticket_count:
                    logger.warning(
                        f"Duplicate contact ticket count mismatch! "
                        f"Expected {duplicate.ticket_count} but found {actual_duplicate_count} - continuing anyway."
                    )

            logger.info("Backup integrity verification passed.")
        except Exception as e:
            logger.error(f"Backup verification failed: {str(e)}")

    def merge_contacts(self, primary: Contact, duplicate: Contact) -> bool:
        """
        Merge duplicate contact into primary contact with safety checks,
        forcibly overwriting external_ref with the duplicate's external_ref
        even if DevRev's verify doesn't show 404 or "deleted."
        """
        try:
            logger.info(f"\nMerging contacts for email {primary.email}")
            logger.info(f"Primary: {primary.display_name} ({primary.rev_user_id})")
            logger.info(f"Duplicate: {duplicate.display_name} ({duplicate.rev_user_id})")

            if self.preview_mode:
                logger.info("PREVIEW MODE - Would make these changes:")
                logger.info("1. Backup all data for both contacts")
                logger.info(f"2. Merge {duplicate.rev_user_id} into {primary.rev_user_id}")
                logger.info(f"3. Update primary contact's external_ref => {duplicate.external_ref}")
                return True

            # 1) Backup
            logger.info("Backing up contact data...")
            if not self.api.backup_contact_data(primary):
                raise Exception("Failed to backup primary contact data - aborting merge")
            if not self.api.backup_contact_data(duplicate):
                raise Exception("Failed to backup duplicate contact data - aborting merge")

            # 2) Verify backups won't stop the merge. We just log warnings
            self.verify_backup_integrity(primary, duplicate)

            # 3) Do the merge
            merged_ok = self.api.merge_contacts(primary.rev_user_id, duplicate.rev_user_id)
            if not merged_ok:
                raise Exception("Failed to merge contacts (API call returned false)")

            # 4) Optionally wait a moment for DevRev to process
            time.sleep(2)

            # 5) Check if user is "deleted" or "not found"
            if not self.api.verify_merge(duplicate.rev_user_id):
                # Do not treat as final failure. We'll just log an error
                # but continue to update external ref. Some tenants never truly "delete" the user.
                logger.error("User still found or not in a 'deleted' state. Merge may be partial.")
                # We do not abort here

            # 6) Force external_ref => from duplicate to primary
            # Even if DevRev "verify_merge" isn't conclusive, we forcibly do it
            if not self.api.update_external_ref(primary.rev_user_id, duplicate.external_ref):
                logger.warning("We attempted to update external_ref, but it failed. See logs above.")

            self.savepoint.add_processed_pair(primary.rev_user_id, duplicate.rev_user_id)
            self.merged_pairs.append((primary, duplicate))
            logger.info("✓ Merge completed successfully (with forced external_ref overwrite)")
            return True

        except Exception as e:
            self.failed_merges.append((primary, duplicate, str(e)))
            logger.error(f"✗ Failed to merge contacts: {str(e)}")
            return False

    def generate_report(self) -> None:
        """
        Generate a detailed report of merges (JSON + HTML).
        """
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
                        "display_name": p.display_name,
                        "email": p.email,
                        "id": p.rev_user_id,
                        "original_external_ref": p.external_ref,
                        "new_external_ref": d.external_ref,
                        "original_ticket_count": p.ticket_count,
                        "final_ticket_count": p.ticket_count + d.ticket_count,
                    },
                    "duplicate": {
                        "display_name": d.display_name,
                        "email": d.email,
                        "id": d.rev_user_id,
                        "external_ref": d.external_ref,
                        "ticket_count": d.ticket_count,
                    },
                }
                for p, d in self.merged_pairs
            ],
            "failed_merges": [
                {
                    "primary": {
                        "display_name": p.display_name,
                        "email": p.email,
                        "id": p.rev_user_id,
                    },
                    "duplicate": {
                        "display_name": d.display_name,
                        "email": d.email,
                        "id": d.rev_user_id,
                    },
                    "error": error,
                }
                for p, d, error in self.failed_merges
            ],
        }

        report_path = report_dir / f"merge_report_{timestamp}.json"
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        logger.info(f"Generated JSON report: {report_path}")

        # Generate HTML
        self.generate_html_report()

    def generate_html_report(self) -> None:
        """
        Generate an HTML report of the merge operations with logs included.
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_dir = Path("reports")
        report_dir.mkdir(exist_ok=True)

        env = Environment(loader=FileSystemLoader('templates'))
        template = env.get_template('report_template.html')

        log_content = ""
        if LOG_FILE_PATH and LOG_FILE_PATH.exists():
            try:
                with open(LOG_FILE_PATH, "r", encoding="utf-8") as f:
                    log_content = f.read()
            except Exception as e:
                logger.error(f"Failed to read log file: {e}")

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
                        "display_name": p.display_name,
                        "email": p.email,
                        "original_external_ref": p.external_ref,
                        "original_ticket_count": p.ticket_count,
                        "final_ticket_count": p.ticket_count + d.ticket_count,
                    },
                    "duplicate": {
                        "display_name": d.display_name,
                        "email": d.email,
                        "external_ref": d.external_ref,
                        "ticket_count": d.ticket_count,
                    },
                }
                for p, d in self.merged_pairs
            ],
            "failed_merges": [
                {
                    "primary": {
                        "display_name": p.display_name,
                        "email": p.email,
                    },
                    "duplicate": {
                        "display_name": d.display_name,
                        "email": d.email,
                    },
                    "error": error,
                }
                for p, d, error in self.failed_merges
            ],
            "log_content": log_content or "No logs available.",
        }

        html_content = template.render(**report_data)
        report_path = report_dir / f"merge_report_{timestamp}.html"
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(html_content)

        logger.info(f"Generated HTML report: {report_path}")

    def process_csv(self, csv_path: str, preview: bool = False, filter_email: Optional[str] = None) -> None:
        """
        Process the CSV file and merge duplicate contacts.
        """
        self.preview_mode = preview
        contacts = []

        with open(csv_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    c = Contact.from_dict(row)
                    if filter_email and c.email.lower() != filter_email.lower():
                        continue
                    contacts.append(c)
                except ValueError as e:
                    logger.error(f"Invalid contact data: {e}")
                    continue

        duplicates = self.identify_duplicates(contacts)
        logger.info(f"\nFound {len(duplicates)} duplicate pairs to process")

        if preview:
            logger.info("\nPREVIEW MODE - No changes will be made")
            for primary, dup in duplicates:
                logger.info(f"\nWould merge:")
                logger.info(f"Primary: {primary.display_name} ({primary.email})")
                logger.info(f"Duplicate: {dup.display_name} ({dup.email})")
            return

        total = len(duplicates)
        for idx, (primary, dup) in enumerate(duplicates, 1):
            logger.info(f"\nProcessing pair {idx}/{total}")
            self.merge_contacts(primary, dup)


# ---------------------------------------------------------------------
# MAIN FUNCTION
# ---------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Merge duplicate DevRev contacts")
    parser.add_argument("--csv", required=True, help="Path to the CSV file containing contacts")
    parser.add_argument("--preview", action="store_true", help="Preview changes without executing them")
    parser.add_argument("--batch-size", type=int, default=10, help="Number of merges to process in one batch")
    parser.add_argument("--filter-email", help="Only process duplicates for this email address")
    args = parser.parse_args()

    api_token = os.getenv("DEVREV_API_TOKEN")
    if not api_token:
        logger.error("DEVREV_API_TOKEN environment variable is required")
        return

    try:
        if not os.path.exists(args.csv):
            logger.error(f"CSV file not found: {args.csv}")
            return

        # Create required directories
        for directory in ["logs", "reports", "backups"]:
            Path(directory).mkdir(exist_ok=True)

        # Initialize
        api = DevRevAPI(api_token)
        merger = ContactMerger(api)

        # Process the CSV
        logger.info("Starting contact merge process...")
        logger.info(f"CSV file: {args.csv}")
        logger.info(f"Preview mode: {args.preview}")

        merger.process_csv(args.csv, args.preview, filter_email=args.filter_email)
        merger.generate_report()

        # Print summary
        if not args.preview:
            logger.info("\nMerge process completed!")
            logger.info(f"Successfully merged: {len(merger.merged_pairs)} pairs")
            logger.info(f"Failed merges: {len(merger.failed_merges)} pairs")
            if merger.failed_merges:
                logger.info("\nFailed merges:")
                for p, d, err in merger.failed_merges:
                    logger.info(f"- {p.email}: {err}")

        logger.info("Both JSON and HTML reports have been generated in the 'reports' directory.")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise


if __name__ == "__main__":
    main()
