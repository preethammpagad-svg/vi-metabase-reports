import os
import requests
import csv
import shutil
import tempfile
import datetime
import smtplib
from email.message import EmailMessage

# -----------------------------
# ENVIRONMENT VARIABLES
# -----------------------------
METABASE_SITE = os.getenv("METABASE_SITE")
METABASE_API_KEY = os.getenv("METABASE_API_KEY")

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
FROM_EMAIL = os.getenv("FROM_EMAIL")
TO_EMAIL = os.getenv("TO_EMAIL")

# List of cards to export
CARDS = [
    ("VI Postpaid: Immediate Callback (D-1)", 1234),   # Replace IDs
    ("VI Postpaid: Disposition History (D-1)", 5678),  # Replace IDs
]


# -----------------------------
# METABASE EXPORT FUNCTION
# -----------------------------
def download_csv(card_id, name, output_dir):
    url = f"{METABASE_SITE}/api/card/{card_id}/query/csv"

    headers = {"X-Metabase-Session": METABASE_API_KEY}

    response = requests.post(url, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Failed to export {name}: {response.text}")

    safe_name = name.replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_")
    filename = f"{safe_name}.csv"
    file_path = os.path.join(output_dir, filename)

    with open(file_path, "wb") as f:
        f.write(response.content)

    print(f"Downloaded: {name} → {file_path}")
    return file_path


# -----------------------------
# SEND EMAIL
# -----------------------------
def send_email(zip_path):
    msg = EmailMessage()
    msg["Subject"] = "VI Daily Reports"
    msg["From"] = FROM_EMAIL
    msg["To"] = TO_EMAIL
    msg.set_content("Please find attached the VI daily reports.")

    # attach zip
    with open(zip_path, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="application",
            subtype="zip",
            filename=os.path.basename(zip_path),
        )

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as smtp:
        smtp.starttls()
        smtp.login(SMTP_USER, SMTP_PASS)
        smtp.send_message(msg)

    print("Email sent!")


# -----------------------------
# MAIN EXECUTION
# -----------------------------
if __name__ == "__main__":
    # Create a temporary directory to store CSV files
    tmp_dir = tempfile.mkdtemp(prefix="metabase_export_")
    print("Temp directory:", tmp_dir)

    # Download all reports
    csv_files = []
    for name, card_id in CARDS:
        csv_path = download_csv(card_id, name, tmp_dir)
        csv_files.append(csv_path)

    # -----------------------------
    # CREATE ZIP IN WORKSPACE (IMPORTANT)
    # -----------------------------
    today = datetime.datetime.utcnow().strftime("%Y%m%d")

    # Zip file name and final output in GitHub runner's workspace
    zip_name = f"VI_Daily_Reports_{today}"
    zip_path = os.path.abspath(f"{zip_name}.zip")

    shutil.make_archive(zip_name, "zip", tmp_dir)

    print("ZIP created:", zip_path)

    # -----------------------------
    # SEND EMAIL WITH ZIP
    # -----------------------------
    print("Sending email...")
    send_email(zip_path)

    # Cleanup temp dir
    shutil.rmtree(tmp_dir)

    print("Done!")
