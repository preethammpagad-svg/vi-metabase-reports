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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
    """Fetch card metadata to get human-readable name."""
    url = f"{METABASE_SITE}/api/card/{card_id}"
    r = session.get(url, verify=VERIFY_SSL, timeout=30)
    r.raise_for_status()
    return r.json().get("name", f"Card_{card_id}")

def build_params(params_dict):
    params = []
    for k, v in (params_dict or {}).items():
        params.append({
            "type": "category",
            "target": ["variable", "template-tag", k],
            "value": v
        })
    return params

def _make_retry_session(retries=3, backoff_factor=1, status_forcelist=(500,502,503,504)):
    s = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(['GET','POST','PUT','DELETE','HEAD','OPTIONS'])
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    # ensure x-api-key header preserved if set on outer session
    return s

def download_card_xlsx(session, card_id, out_path, params=None, read_timeout=300):
    """
    Download card results as XLSX using streaming and retries.
    - session: requests.Session that carries x-api-key header (we'll reuse headers).
    - out_path: local file path to write.
    - read_timeout: seconds allowed for read; default 300 (5 minutes).
    """
    url = f"{METABASE_SITE}/api/card/{card_id}/query/xlsx"
    payload = {}
    if params:
        payload["parameters"] = build_params(params)

    # use a local session that shares headers from the provided session
    s = _make_retry_session(retries=3, backoff_factor=1)
    s.headers.update(session.headers)

    # stream the response to avoid timeouts while building content in memory
    with s.post(url, json=payload, verify=VERIFY_SSL, stream=True, timeout=(10, read_timeout)) as r:
        r.raise_for_status()
        # write in chunks
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

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
    xlsx_paths = []

    try:
        for cid in CARD_IDS:
            card_name = get_card_name(session, cid)
            clean_name = clean_filename(card_name)
            xlsx_path = os.path.join(tmpdir, f"{clean_name}.xlsx")

            print(f"Downloading (XLSX): {card_name} → {xlsx_path}")

            params = CARD_PARAMS.get(str(cid)) or CARD_PARAMS.get(cid) or None
            download_card_xlsx(session, cid, xlsx_path, params=params)
            xlsx_paths.append(xlsx_path)

        # Create final zip
        zip_path = os.path.join(
            tmpdir,
            f"VI_Daily_Reports_{datetime.now().strftime('%Y%m%d')}.zip"
        )
        make_zip(xlsx_paths, zip_path)

        print("Sending email...")
        send_email(zip_path)
        print("Email sent!")

    finally:
        for f in xlsx_paths:
            try: os.remove(f)
            except: pass
        try: os.remove(zip_path)
        except: pass

if __name__ == "__main__":
    main()
