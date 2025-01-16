import csv
from collections import Counter

with open("contacts.csv", newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    emails = [row["EMAIL"].lower() for row in reader]

email_counts = Counter(emails)
duplicate_emails = {email: count for email, count in email_counts.items() if count > 1}
print(f"Total unique emails: {len(email_counts)}")
print(f"Emails with duplicates: {len(duplicate_emails)}")
