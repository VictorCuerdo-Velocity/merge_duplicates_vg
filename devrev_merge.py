#!/usr/bin/env python3
import csv
import os
import json
import argparse
import logging
import time
import requests
from datetime import datetime
from ratelimit import limits, sleep_and_retry

# --- Configuration ---
DEVREV_API_URL = "https://api.devrev.ai"  # Base API URL
API_TOKEN = os.getenv("DEVREV_API_TOKEN")
# Adjust API rate limits if necessary
CALLS_PER_MINUTE = 300
PERIOD = 60

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- Rate Limited API call ---
@sleep_and_retry
@limits(calls=CALLS_PER_MINUTE, period=PERIOD)
def api_call(method, endpoint, payload=None):
    url = f"{DEVREV_API_URL}{endpoint}"
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    if method.upper() == "GET":
        response = requests.get(url, headers=headers, params=payload)
    else:
        response = requests.request(method.upper(), url, headers=headers, json=payload)
    if response.status_code not in (200, 201):
        logger.error(f"Error: {response.status_code} {response.text}")
    return response

# --- Merge Contacts Logic ---
def merge_contacts(contact_a, contact_b):
    """
    Merge duplicate contact (contact_b) into primary contact (contact_a).

    contact_a: dict with fields {display_name, email, external_ref, total_ticket_count}
    contact_b: dict with fields {display_name, email, external_ref, total_ticket_count}

    In this example, we update contact_a's external_ref to contact_b's external_ref
    and update any other fields as required. Then, we optionally delete contact_b.
    """
    # Prepare payload for updating Contact A:
    updated_external_ref = contact_b["external_ref"]
    updated_ticket_count = int(contact_a["total_ticket_count"]) + int(contact_b["total_ticket_count"])
    
    update_payload = {
        "id": contact_a["DEV_USER_ID"],
        "display_name": contact_a["DISPLAY_NAME"],  # primary retains proper name
        "external_ref": updated_external_ref,
        # You may pass additional fields (e.g., ticket count) if the API supports it.
    }
    
    logger.info(f"Updating contact {contact_a['EMAIL']} (ID: {contact_a['DEV_USER_ID']}) "
                f"with external_ref {updated_external_ref} and ticket_count {updated_ticket_count}")
    
    response = api_call("POST", "/rev-users.update", update_payload)
    if response.ok:
        logger.info(f"Contact {contact_a['EMAIL']} updated successfully.")
        # Optionally, call the delete endpoint for contact B:
        delete_payload = { "id": contact_b["DEV_USER_ID"] }
        del_response = api_call("POST", "/rev-users.delete", delete_payload)
        if del_response.ok:
            logger.info(f"Duplicate contact {contact_b['EMAIL']} (ID: {contact_b['DEV_USER_ID']}) deleted.")
        else:
            logger.error(f"Failed to delete duplicate contact: {contact_b['DEV_USER_ID']}")
    else:
        logger.error(f"Failed to update primary contact: {contact_a['DEV_USER_ID']}")

# --- Process CSV and Identify Duplicates ---
def process_contacts_csv(csv_file):
    contacts = {}
    duplicates = []
    # Read CSV: we assume that the CSV contains columns like:
    # DEV_USER_ID, DISPLAY_NAME, EMAIL, EXTERNAL_REF, total_ticket_count
    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            email = row["EMAIL"].strip().lower()
            # Collect duplicates by email
            if email in contacts:
                contacts[email].append(row)
            else:
                contacts[email] = [row]

    # Now iterate over each email and decide if there are duplicates.
    for email, contact_list in contacts.items():
        if len(contact_list) == 2:
            # For this scenario, assume that the row where DISPLAY_NAME looks like a proper person name
            # (e.g., contains a space) is the primary. Otherwise, the one whose display_name equals the email is the duplicate.
            candidate_a = None
            candidate_b = None
            for contact in contact_list:
                if "@" not in contact["DISPLAY_NAME"]:
                    candidate_a = contact
                else:
                    candidate_b = contact
            if candidate_a and candidate_b:
                duplicates.append((candidate_a, candidate_b))
            else:
                logger.info(f"Could not determine primary contact for {email}")
        else:
            logger.info(f"No duplicates found for {email}")
    
    logger.info(f"Found {len(duplicates)} duplicate pairs.")
    
    # Merge all duplicate pairs
    for contact_a, contact_b in duplicates:
        merge_contacts(contact_a, contact_b)

# --- Main function ---
def main():
    parser = argparse.ArgumentParser(description="Merge duplicate internal contacts in DevRev based on CSV data.")
    parser.add_argument("--csv", required=True, help="Path to the contacts CSV file")
    args = parser.parse_args()
    
    if not API_TOKEN:
        logger.error("DEVREV_API_TOKEN environment variable is required.")
        return
    
    process_contacts_csv(args.csv)

if __name__ == "__main__":
    main()
