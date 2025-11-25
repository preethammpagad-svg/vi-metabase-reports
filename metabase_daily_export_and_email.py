#!/usr/bin/env python3
import os
import requests
import tempfile
import zipfile
import smtplib
import ssl
from email.message import EmailMessage
from datetime import datetime
import re
import time

# ---------- Environment ----------
METABASE_SITE = os.getenv("METABASE_SITE", "https://metabase.skit.ai").rstrip("/")
METABASE_API_KEY = os.getenv("METABASE_API_KEY")

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
FROM_EMAIL = os.getenv("FROM_EMAIL")
TO_EMAIL = os.getenv("TO_EMAIL")

CARD_IDS = [8266, 8267]
CARD_PARAMS = {}
VERIFY_SSL = True

# ---------- Retry / timeout settings ----------
MAX_ATTEMPTS = 5            # total attempts per request
INITIAL_BACKOFF = 3        # seconds before first retry
BACKOFF_BASE = 2           # exponential base (2 -> 3s, 6s, 12s, 24s...)
REQUEST_TIMEOUT = 180     # seconds to wait for each request (increased from 60)

# ---------- Helpers ----------
def clean_filename(name):
    """Sanitize card name so it can be used as a filename."""
    return re.sub(r'[^A-Za-z0-9]+', '_', name).strip('_')

def make_session():
    if not METABASE_API_KEY:
        raise RuntimeError("METABASE_API_KEY is required.")
    s = requests.Session()
    s.headers.update({"x-api-key": METABASE_API_KEY})
    return s

def get_card_name(session, card_id):
    """Fetch card metadata to get human-readable name. Retries on transient failures."""
    url = f"{METABASE_SITE}/api/card/{card_id}"
    attempt = 0
    while attempt < MAX_ATTEMPTS:
        attempt += 1
        try:
            print(f"Fetching card metadata (id={card_id}) attempt {attempt}/{MAX_ATTEMPTS}")
            r = session.get(url, verify=VERIFY_SSL, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            return data.get("name", f"Card_{card_id}")
        except requests.exceptions.RequestException as e:
            # Treat as transient for retries (timeouts, connection errors, 5xx)
            if attempt >= MAX_ATTEMPTS:
                print(f"ERROR: failed to fetch metadata for card {card_id}: {e}")
                raise
            wait = INITIAL_BACKOFF * (BACKOFF_BASE ** (attempt - 1))
            print(f"Warning: failed to fetch metadata for card {card_id}: {e}. Retrying in {wait}s...")
            time.sleep(wait)

def build_params(params_dict):
    params = []
    for k, v in (params_dict or {}).items():
        params.append({
            "type": "category",
            "target": ["variable", "template-tag", k],
            "value": v
        })
    return params

def download_card_csv(session, card_id, out_path, params=None):
    """Download card as CSV with retries on transient errors."""
    url = f"{METABASE_SITE}/api/card/{card_id}/query/csv"
    payload = {}
    if params:
        payload["parameters"] = build_params(params)

    attempt = 0
    while attempt < MAX_ATTEMPTS:
        attempt += 1
        try:
            print(f"Downloading (CSV) card {card_id} attempt {attempt}/{MAX_ATTEMPTS} -> {out_path}")
            r = session.post(url, json=payload, verify=VERIFY_SSL, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            with open(out_path, "wb") as f:
                f.write(r.content)
            size = os.path.getsize(out_path)
            print(f"Saved {out_path} ({size} bytes)")
            return
        except requests.exceptions.RequestException as e:
            if attempt >= MAX_ATTEMPTS:
                print(f"ERROR: failed to download CSV for card {card_id}: {e}")
                raise
            wait = INITIAL_BACKOFF * (BACKOFF_BASE ** (attempt - 1))
            print(f"Warning: download failed for card {card_id}: {e}. Retrying in {wait}s...")
            time.sleep(wait)

def make_zip(files, zip_path):
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for p in files:
            z.write(p, arcname=os.path.basename(p))

def send_email(zip_path):
    subject = "VI Daily Reports (D-1)"
    body = (
        "Hi Team,\n\n"
        "Please find Immediate Callback Report & Disposition History Report "
        "for yesterday's campaigns attached below.\n\n"
        "Thank You"
    )

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = FROM_EMAIL
    msg["To"] = TO_EMAIL
    msg.set_content(body)

    with open(zip_path, "rb") as f:
        data = f.read()

    msg.add_attachment(
        data,
        maintype="application",
        subtype="zip",
        filename=os.path.basename(zip_path)
    )

    context = ssl.create_default_context()
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as smtp:
        smtp.starttls(context=context)
        smtp.login(SMTP_USER, SMTP_PASS)
        smtp.send_message(msg)

# ---------- Main ----------
def main():
    session = make_session()
    tmpdir = tempfile.mkdtemp(prefix="metabase_export_")
    csv_paths = []

    try:
        for cid in CARD_IDS:
            # Fetch card name
            card_name = get_card_name(session, cid)
            clean_name = clean_filename(card_name)
            csv_path = os.path.join(tmpdir, f"{clean_name}.csv")

            print(f"Downloading: {card_name} → {csv_path}")

            params = CARD_PARAMS.get(str(cid)) or CARD_PARAMS.get(cid) or None
            download_card_csv(session, cid, csv_path, params=params)
            csv_paths.append(csv_path)

        # Create final zip
        zip_path = os.path.join(
            tmpdir,
            f"VI_Daily_Reports_{datetime.now().strftime('%Y%m%d')}.zip"
        )
        make_zip(csv_paths, zip_path)

        print("Sending email...")
        send_email(zip_path)
        print("Email sent!")

    finally:
        for f in csv_paths:
            try: os.remove(f)
            except: pass
        try: os.remove(zip_path)
        except: pass

if __name__ == "__main__":
    main()
