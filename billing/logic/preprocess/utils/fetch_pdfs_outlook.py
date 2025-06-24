#!/usr/bin/env python
# fetch_pdfs_graph.py
# ---------------------------------------------------------------
# Download every PDF from   Inbox/providerbills
# then move each processed message into Inbox/providerbills/archive_bills
#
# Works for a *single* mailbox but you can loop over many.
# ---------------------------------------------------------------
import os, json, requests, sys, time
from pathlib import Path
from msal import ConfidentialClientApplication
from dotenv import load_dotenv

# Load .env from root directory
root_dir = Path(__file__).parent.parent.parent.parent.parent
load_dotenv(root_dir / '.env')   # Load from root directory

# ── 1) AUTH ─────────────────────────────────────────────────────
CLIENT_ID     = os.getenv("GRAPH_CLIENT_ID")
CLIENT_SECRET = os.getenv("GRAPH_CLIENT_SECRET")
TENANT_ID     = os.getenv("GRAPH_TENANT_ID")

app = ConfidentialClientApplication(
    client_id          = CLIENT_ID,
    client_credential  = CLIENT_SECRET,
    authority          = f"https://login.microsoftonline.com/{TENANT_ID}"
)

token = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
if "access_token" not in token:
    sys.exit(f"❌ Couldn't obtain token: {token.get('error_description')}")

HEADERS = {"Authorization": f"Bearer {token['access_token']}",
           "Accept":        "application/json"}

# ── 2) SETTINGS ─────────────────────────────────────────────────
MAILBOX         = "christopher.cato@clarity-dx.com"
SRC_PATH        = ["Inbox", "providerbills"]            # parent → child
DEST_FOLDERNAME = "archive_bills"
DOWNLOAD_DIR    = Path(r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\code\monolith\billing\data\billbatch")
PAGE_SIZE       = 50                                    # Graph page size
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

# ── 3) UTILS ───────────────────────────────────────────────────
GRAPH_ROOT = "https://graph.microsoft.com/v1.0"


def get_folder_id(user, path_segments):
    """Walk a human path like ['Inbox','foo','bar'] and return folderId."""
    # First segment may be special well-known folder; get its id quickly
    seg0 = path_segments[0]
    resp = requests.get(f"{GRAPH_ROOT}/users/{user}/mailFolders/{seg0}",
                        headers=HEADERS)
    if resp.status_code != 200:
        raise RuntimeError(f"Cannot find top folder {seg0}: {resp.text}")
    folder = resp.json()
    for name in path_segments[1:]:
        escaped_name = name.replace("'", "''")
        q = (f"{GRAPH_ROOT}/users/{user}/mailFolders/{folder['id']}/childFolders"
             f"?$filter=displayName eq '{escaped_name}'"
             f"&$select=id,displayName")
        resp = requests.get(q, headers=HEADERS)
        data = resp.json().get("value", [])
        if not data:
            # create it and continue
            body = {"displayName": name, "isHidden": False}
            resp = requests.post(f"{GRAPH_ROOT}/users/{user}/mailFolders/{folder['id']}/childFolders",
                                 headers={**HEADERS, "Content-Type": "application/json"},
                                 data=json.dumps(body))
            if resp.status_code not in (200, 201):
                raise RuntimeError(f"Cannot create/find folder {name}: {resp.text}")
            data = [resp.json()]
        folder = data[0]
    return folder["id"]


def iter_messages(user, folder_id):
    """Yield messages page-by-page."""
    url = (f"{GRAPH_ROOT}/users/{user}/mailFolders/{folder_id}/messages"
           f"?$select=id,subject,hasAttachments,receivedDateTime&$top={PAGE_SIZE}")
    while url:
        resp = requests.get(url, headers=HEADERS)
        if resp.status_code != 200:
            raise RuntimeError(f"Message fetch failed: {resp.text}")
        data = resp.json()
        for m in data.get("value", []):
            yield m
        url = data.get("@odata.nextLink")


def save_pdf_attachment(user, msg_id, att):
    """Download a single attachment if it's a PDF."""
    att_id   = att["id"]
    filename = att["name"]
    if not filename.lower().endswith(".pdf"):
        return False
    # Small attachments: contentBytes is already present (> 4 MB need /$value)
    if "contentBytes" in att:
        payload = att["contentBytes"]
        bin_data = bytes(payload, "utf-8") if isinstance(payload, str) else payload
    else:
        url = f"{GRAPH_ROOT}/users/{user}/messages/{msg_id}/attachments/{att_id}/$value"
        r   = requests.get(url, headers=HEADERS)
        if r.status_code != 200:
            print(f"⚠️  Cannot download {filename}: {r.text}")
            return False
        bin_data = r.content

    target = DOWNLOAD_DIR / filename
    counter = 1
    while target.exists():
        target = target.with_stem(f"{target.stem}_{counter}")
        counter += 1
    target.write_bytes(bin_data)
    return True


def move_message(user, msg_id, dest_folder_id):
    url  = f"{GRAPH_ROOT}/users/{user}/messages/{msg_id}/move"
    body = {"destinationId": dest_folder_id}
    resp = requests.post(url, headers={**HEADERS, "Content-Type": "application/json"},
                         data=json.dumps(body))
    return resp.status_code in (200, 201)


# ── 4) MAIN ─────────────────────────────────────────────────────
def fetch_pdfs():
    print(f"🔍 Looking for messages in {MAILBOX}/{'/'.join(SRC_PATH)}")
    src_id  = get_folder_id(MAILBOX, SRC_PATH)
    print(f"📁 Source folder ID: {src_id}")
    
    dest_id = get_folder_id(MAILBOX, SRC_PATH + [DEST_FOLDERNAME])
    print(f"📁 Destination folder ID: {dest_id}")

    processed = 0
    message_count = 0
    for msg in iter_messages(MAILBOX, src_id):
        message_count += 1
        print(f"\n📧 Processing message: {msg.get('subject', '(no subject)')}")
        print(f"   Received: {msg.get('receivedDateTime')}")
        print(f"   Has Attachments: {msg.get('hasAttachments')}")
        
        # Fetch attachment metadata
        aurl = f"{GRAPH_ROOT}/users/{MAILBOX}/messages/{msg['id']}/attachments?$select=id,name,contentType,size"
        aresp = requests.get(aurl, headers=HEADERS)
        if aresp.status_code != 200:
            print(f"⚠️  Failed to fetch attachments: {aresp.text}")
            continue
            
        atts = aresp.json().get("value", [])
        print(f"📎 Found {len(atts)} attachments:")
        for att in atts:
            print(f"   - {att.get('name')} ({att.get('contentType')}, {att.get('size')} bytes)")
        
        saved = False
        for att in atts:
            if save_pdf_attachment(MAILBOX, msg["id"], att):
                saved = True
        if saved:
            moved = move_message(MAILBOX, msg["id"], dest_id)
            state = "✅ saved & moved" if moved else "⚠️  saved, move failed"
            processed += 1
            print(f"{state}: {msg.get('subject','(no subject)')[:60]}")
    
    print(f"\n📊 Summary:")
    print(f"- Total messages found: {message_count}")
    print(f"- Messages with attachments: {processed}")
    print(f"- PDFs downloaded to: {DOWNLOAD_DIR}")
    return processed

__all__ = ['fetch_pdfs']

if __name__ == "__main__":
    fetch_pdfs()