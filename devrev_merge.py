#!/usr/bin/env python3

import csv
import os
import json
import argparse
import logging
import time
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import requests
from dotenv import load_dotenv
from ratelimit import limits, sleep_and_retry

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(
            f'contact_merge_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        ),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

class DevRevAPI:
    def __init__(self, api_token: str, base_url: str = "https://api.devrev.ai"):
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        self.session = requests.Session()

    @sleep_and_retry
    @limits(calls=50, period=60)
    def make_request(self, method: str, endpoint: str, data: Optional[Dict] = None) -> requests.Response:
        """Make a rate-limited API request to DevRev"""
        url = f"{self.base_url}{endpoint}"
        try:
            response = self.session.request(method, url, headers=self.headers, json=data)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise

    def get_contact_details(self, contact_id: str) -> Dict:
        """Get detailed information about a contact"""
        response = self.make_request("GET", f"/rev-users.get?id={contact_id}")
        return response.json()["rev_user"]

    def update_contact(self, contact_id: str, update_data: Dict) -> Dict:
        """Update a contact's information"""
        payload = {"id": contact_id, **update_data}
        response = self.make_request("POST", "/rev-users.update", payload)
        return response.json()

    def delete_contact(self, contact_id: str) -> bool:
        """Delete a contact"""
        try:
            self.make_request("POST", "/rev-users.delete", {"id": contact_id})
            return True
        except requests.exceptions.RequestException:
            return False

class ContactMerger:
    def __init__(self, api: DevRevAPI):
        self.api = api
        self.merged_pairs: List[Tuple[str, str]] = []
        self.failed_merges: List[Tuple[str, str, str]] = []

    def is_valid_name(self, name: str) -> bool:
        """Check if a name appears to be a proper name rather than an email address"""
        return (
            name 
            and "@" not in name 
            and len(name.split()) > 1  # Has at least two parts (first and last name)
            and any(char.isalpha() for char in name)  # Contains at least one letter
        )

    def determine_primary_contact(self, contacts: List[Dict]) -> Tuple[Optional[Dict], Optional[Dict]]:
        """
        Determine which contact should be primary based on our rules:
        - Proper name in display_name
        - Has REVU-style external reference
        - Higher ticket count
        """
        proper_name_contacts = [c for c in contacts if self.is_valid_name(c["DISPLAY_NAME"])]
        revu_contacts = [c for c in contacts if c["EXTERNAL_REF"] and c["EXTERNAL_REF"].startswith("REVU-")]
        
        if not proper_name_contacts:
            logger.warning(f"No contact with proper name found for email {contacts[0]['EMAIL']}")
            return None, None

        # Prioritize contacts that have both proper name and REVU- reference
        primary_candidates = [c for c in proper_name_contacts if c in revu_contacts]
        
        if not primary_candidates:
            # Fall back to any contact with a proper name
            primary_candidates = proper_name_contacts

        if not primary_candidates:
            logger.warning(f"No suitable primary contact found for email {contacts[0]['EMAIL']}")
            return None, None

        # Choose the one with the highest ticket count
        primary = max(primary_candidates, key=lambda x: int(x["TICKET_COUNT"]))
        duplicates = [c for c in contacts if c["REV_USER_ID"] != primary["REV_USER_ID"]]
        
        if not duplicates:
            return None, None

        return primary, duplicates[0]

    def merge_contacts(self, primary: Dict, duplicate: Dict) -> bool:
        """
        Merge duplicate contact into primary contact:
        1. Update primary's external_ref to duplicate's external_ref
        2. Delete the duplicate contact
        """
        try:
            logger.info(f"Merging contacts for email {primary['EMAIL']}")
            logger.info(f"Primary: {primary['DISPLAY_NAME']} ({primary['REV_USER_ID']})")
            logger.info(f"Duplicate: {duplicate['DISPLAY_NAME']} ({duplicate['REV_USER_ID']})")

            # Update primary contact with duplicate's external_ref
            update_data = {
                "external_ref": duplicate["EXTERNAL_REF"]
            }
            self.api.update_contact(primary["REV_USER_ID"], update_data)
            
            # Delete duplicate contact
            if self.api.delete_contact(duplicate["REV_USER_ID"]):
                self.merged_pairs.append((primary["REV_USER_ID"], duplicate["REV_USER_ID"]))
                logger.info("Merge completed successfully")
                return True
            else:
                raise Exception("Failed to delete duplicate contact")

        except Exception as e:
            self.failed_merges.append((primary["REV_USER_ID"], duplicate["REV_USER_ID"], str(e)))
            logger.error(f"Failed to merge contacts: {str(e)}")
            return False

    def process_csv(self, csv_path: str) -> None:
        """Process the CSV file and merge duplicate contacts"""
        contacts_by_email: Dict[str, List[Dict]] = {}
        
        # Read and group contacts by email
        with open(csv_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                email = row["EMAIL"].strip().lower()
                contacts_by_email.setdefault(email, []).append(row)

        # Process each group of contacts
        for email, contacts in contacts_by_email.items():
            if len(contacts) < 2:
                continue

            primary, duplicate = self.determine_primary_contact(contacts)
            if primary and duplicate:
                self.merge_contacts(primary, duplicate)

    def generate_report(self) -> None:
        """Generate a report of the merge operations"""
        report = {
            "total_merges_attempted": len(self.merged_pairs) + len(self.failed_merges),
            "successful_merges": len(self.merged_pairs),
            "failed_merges": len(self.failed_merges),
            "merged_pairs": self.merged_pairs,
            "failed_pairs": self.failed_merges
        }

        with open(f"merge_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", 'w') as f:
            json.dump(report, f, indent=2)

def main():
    parser = argparse.ArgumentParser(description="Merge duplicate DevRev contacts")
    parser.add_argument("--csv", required=True, help="Path to the CSV file containing contacts")
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run without making changes")
    args = parser.parse_args()

    api_token = os.getenv("DEVREV_API_TOKEN")
    if not api_token:
        logger.error("DEVREV_API_TOKEN environment variable is required")
        return

    try:
        api = DevRevAPI(api_token)
        merger = ContactMerger(api)
        merger.process_csv(args.csv)
        merger.generate_report()
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()