# email_fetcher.py
"""
Email fetcher for assignment@clarity-dx.com inbox/assigned folder
"""
import os
import requests
import msal
import sqlite3
import tempfile
import mimetypes
import uuid
from datetime import datetime, timedelta
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ClarityEmailFetcher:
    def __init__(self):
        """Initialize with environment variables."""
        self.config = {
            'client_id': os.environ.get('GRAPH_CLIENT_ID'),
            'client_secret': os.environ.get('GRAPH_CLIENT_SECRET'),
            'tenant_id': os.environ.get('GRAPH_TENANT_ID'),
            'shared_mailbox': os.environ.get('SHARED_MAILBOX', 'assignment@clarity-dx.com'),
            'folder_name': os.environ.get('MAILBOX_FOLDER', 'assigned'),
            'scopes': ['https://graph.microsoft.com/.default']
        }
        
        self.access_token = None
        self.db_path = os.environ.get('REFERRALS_DB', 'referrals_wc.db')
        self.s3_bucket = os.environ.get('S3_BUCKET')
        
        # Import boto3 for S3 operations
        import boto3
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
            region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-2')
        )
        
        # Validate required config
        required_fields = ['client_id', 'client_secret', 'tenant_id']
        for field in required_fields:
            if not self.config[field]:
                raise ValueError(f"Missing required environment variable: {field.upper()}")
    
    def authenticate(self):
        """Authenticate with Microsoft Graph API."""
        app = msal.ConfidentialClientApplication(
            self.config['client_id'],
            authority=f"https://login.microsoftonline.com/{self.config['tenant_id']}",
            client_credential=self.config['client_secret']
        )
        
        result = app.acquire_token_for_client(scopes=self.config['scopes'])
        
        if "access_token" in result:
            self.access_token = result["access_token"]
            logger.info("Successfully authenticated with Microsoft Graph")
            return True
        else:
            logger.error(f"Authentication failed: {result.get('error')}")
            logger.error(f"Error description: {result.get('error_description')}")
            return False
    
    def get_folder_id(self):
        """Get the folder ID for the 'assigned' folder under Inbox."""
        if not self.access_token:
            if not self.authenticate():
                return None
        
        # First get the Inbox folder
        endpoint = f"https://graph.microsoft.com/v1.0/users/{self.config['shared_mailbox']}/mailFolders"
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        
        response = requests.get(endpoint, headers=headers)
        
        if response.status_code != 200:
            logger.error(f"Error getting folders: {response.status_code} - {response.text}")
            return None
        
        folders = response.json().get('value', [])
        inbox_id = None
        
        # Find Inbox folder
        for folder in folders:
            if folder.get('displayName', '').lower() == 'inbox':
                inbox_id = folder.get('id')
                break
        
        if not inbox_id:
            logger.error("Could not find Inbox folder")
            return None
        
        # Now get subfolders of Inbox to find 'assigned'
        subfolder_endpoint = f"https://graph.microsoft.com/v1.0/users/{self.config['shared_mailbox']}/mailFolders/{inbox_id}/childFolders"
        response = requests.get(subfolder_endpoint, headers=headers)
        
        if response.status_code != 200:
            logger.error(f"Error getting subfolders: {response.status_code} - {response.text}")
            return None
        
        subfolders = response.json().get('value', [])
        
        # Find 'assigned' folder
        for folder in subfolders:
            if folder.get('displayName', '').lower() == self.config['folder_name'].lower():
                folder_id = folder.get('id')
                logger.info(f"Found '{self.config['folder_name']}' folder with ID: {folder_id}")
                return folder_id
        
        logger.error(f"Could not find '{self.config['folder_name']}' folder under Inbox")
        return None
    
    def get_unprocessed_emails(self, days=7, max_emails=50):
        """Get unprocessed emails from the assigned folder."""
        if not self.access_token:
            if not self.authenticate():
                return []
        
        folder_id = self.get_folder_id()
        if not folder_id:
            return []
        
        # Calculate date filter
        date_filter = (datetime.utcnow() - timedelta(days=days)).strftime('%Y-%m-%dT%H:%M:%SZ')
        
        endpoint = f"https://graph.microsoft.com/v1.0/users/{self.config['shared_mailbox']}/mailFolders/{folder_id}/messages"
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        
        params = {
            '$top': max_emails,
            '$filter': f"receivedDateTime ge {date_filter}",
            '$orderby': 'receivedDateTime desc',
            '$select': 'id,subject,sender,receivedDateTime,hasAttachments,bodyPreview,body'
        }
        
        response = requests.get(endpoint, headers=headers, params=params)
        
        if response.status_code == 200:
            emails = response.json().get('value', [])
            logger.info(f"Found {len(emails)} emails in '{self.config['folder_name']}' folder")
            return emails
        else:
            logger.error(f"Error fetching emails: {response.status_code} - {response.text}")
            return []
    
    def get_email_attachments(self, email_id):
        """Get attachments for a specific email."""
        if not self.access_token:
            if not self.authenticate():
                return []
        
        endpoint = f"https://graph.microsoft.com/v1.0/users/{self.config['shared_mailbox']}/messages/{email_id}/attachments"
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        
        response = requests.get(endpoint, headers=headers)
        
        if response.status_code == 200:
            attachments = response.json().get('value', [])
            logger.info(f"Found {len(attachments)} attachments for email {email_id}")
            return attachments
        else:
            logger.error(f"Error fetching attachments: {response.status_code} - {response.text}")
            return []
    
    def download_attachment(self, email_id, attachment_id):
        """Download a specific attachment."""
        if not self.access_token:
            if not self.authenticate():
                return None
        
        endpoint = f"https://graph.microsoft.com/v1.0/users/{self.config['shared_mailbox']}/messages/{email_id}/attachments/{attachment_id}"
        headers = {
            'Authorization': f'Bearer {self.access_token}'
        }
        
        response = requests.get(endpoint, headers=headers)
        
        if response.status_code == 200:
            attachment_data = response.json()
            if 'contentBytes' in attachment_data:
                import base64
                return base64.b64decode(attachment_data['contentBytes'])
            else:
                logger.error("Attachment doesn't contain contentBytes")
                return None
        else:
            logger.error(f"Error downloading attachment: {response.status_code} - {response.text}")
            return None
    
    def upload_to_s3(self, file_content, attachment_data, email_db_id):
        """Upload attachment to S3 with UUID filename."""
        try:
            # Generate UUID for unique filename
            file_uuid = str(uuid.uuid4())
            original_filename = attachment_data.get('name', f"attachment_{attachment_data['id']}")
            
            # Get file extension
            _, ext = os.path.splitext(original_filename)
            
            # Create S3 key: referrals/attachments/uuid.ext
            s3_key = f"referrals/attachments/{file_uuid}{ext}"
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=file_content,
                ContentType=attachment_data.get('contentType', 'application/octet-stream'),
                Metadata={
                    'original_filename': original_filename,
                    'referral_id': str(email_db_id),
                    'outlook_attachment_id': attachment_data['id']
                }
            )
            
            logger.info(f"Uploaded attachment to S3: s3://{self.s3_bucket}/{s3_key}")
            return s3_key
            
        except Exception as e:
            logger.error(f"Error uploading to S3: {str(e)}")
            return None
    
    def save_email_to_db(self, email_data):
        """Save email metadata to the database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Check if email already exists
            cursor.execute("SELECT id FROM outlook_messages WHERE message_id = ?", (email_data['id'],))
            if cursor.fetchone():
                logger.info(f"Email {email_data['id']} already exists in database")
                return None
            
            # Insert new email
            cursor.execute('''
                INSERT INTO outlook_messages (
                    message_id, subject, sender_email, sender_name, 
                    received_datetime, body_preview, body_content, 
                    has_attachments, processing_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                email_data['id'],
                email_data.get('subject', ''),
                email_data.get('sender', {}).get('emailAddress', {}).get('address', ''),
                email_data.get('sender', {}).get('emailAddress', {}).get('name', ''),
                email_data.get('receivedDateTime', ''),
                email_data.get('bodyPreview', ''),
                email_data.get('body', {}).get('content', ''),
                email_data.get('hasAttachments', False),
                'pending'
            ))
            
            email_db_id = cursor.lastrowid
            conn.commit()
            logger.info(f"Saved email {email_data['id']} to database with ID {email_db_id}")
            return email_db_id
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error saving email to database: {str(e)}")
            return None
        finally:
            conn.close()
    
    def save_attachment_to_db(self, email_db_id, attachment_data, s3_key=None):
        """Save attachment metadata to the database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO attachments (
                    message_id, outlook_attachment_id, filename, 
                    content_type, size, s3_key, upload_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                email_db_id,
                attachment_data['id'],
                attachment_data.get('name', ''),
                attachment_data.get('contentType', ''),
                attachment_data.get('size', 0),
                s3_key,
                'uploaded' if s3_key else 'pending'
            ))
            
            conn.commit()
            logger.info(f"Saved attachment {attachment_data.get('name', '')} to database")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error saving attachment to database: {str(e)}")
        finally:
            conn.close()
    
    def process_emails(self, days=7, max_emails=50):
        """Main method to process emails from the assigned folder."""
        logger.info(f"Starting email processing for {self.config['shared_mailbox']}/{self.config['folder_name']}")
        
        # Get emails
        emails = self.get_unprocessed_emails(days=days, max_emails=max_emails)
        
        for email in emails:
            email_id = email['id']
            logger.info(f"Processing email: {email.get('subject', 'No Subject')}")
            
            # Save email to database
            email_db_id = self.save_email_to_db(email)
            if not email_db_id:
                continue
            
            # Process attachments if any
            if email.get('hasAttachments', False):
                attachments = self.get_email_attachments(email_id)
                
                for attachment in attachments:
                    logger.info(f"Processing attachment: {attachment.get('name', 'Unknown')}")
                    
                    # For now, just save metadata (you can add S3 upload later)
                    self.save_attachment_to_db(email_db_id, attachment)
                    
                    # Download and upload to S3
                    file_content = self.download_attachment(email_id, attachment['id'])
                    if file_content:
                        s3_key = self.upload_to_s3(file_content, attachment, email_db_id)
                        if s3_key:
                            self.save_attachment_to_db(email_db_id, attachment, s3_key)
                        else:
                            self.save_attachment_to_db(email_db_id, attachment)
        
        logger.info("Email processing completed")

if __name__ == "__main__":
    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()
    
    # Create database if it doesn't exist
    if not Path('referrals_wc.db').exists():
        from create_referrals_database import create_referrals_database
        create_referrals_database()
    
    # Process emails
    fetcher = ClarityEmailFetcher()
    fetcher.process_emails(days=7, max_emails=20)