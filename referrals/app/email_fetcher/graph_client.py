# monolith/referrals/app/email_fetcher/graph_client.py
"""
Microsoft Graph API client for fetching emails from Outlook shared inbox.
"""
import os
import requests
from datetime import datetime, timedelta
import msal
import json

class GraphAPIClient:
    def __init__(self, config_path=None):
        """Initialize the Graph API client with authentication parameters."""
        if config_path:
            with open(config_path, 'r') as f:
                self.config = json.load(f)
        else:
            # Default config from environment variables
            self.config = {
                'client_id': os.environ.get('GRAPH_CLIENT_ID'),
                'client_secret': os.environ.get('GRAPH_CLIENT_SECRET'),
                'tenant_id': os.environ.get('GRAPH_TENANT_ID'),
                'shared_mailbox': os.environ.get('SHARED_MAILBOX'),
                'scopes': ['https://graph.microsoft.com/.default']
            }
        
        self.access_token = None
    
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
            return True
        else:
            print(f"Authentication failed: {result.get('error')}")
            print(f"Error description: {result.get('error_description')}")
            return False
    
    def get_unprocessed_emails(self, days=1, max_emails=50):
        """Get unprocessed emails from the shared mailbox."""
        if not self.access_token:
            if not self.authenticate():
                return []
        
        # Calculate date filter
        date_filter = (datetime.utcnow() - timedelta(days=days)).strftime('%Y-%m-%dT%H:%M:%SZ')
        
        endpoint = f"https://graph.microsoft.com/v1.0/users/{self.config['shared_mailbox']}/messages"
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        
        params = {
            '$top': max_emails,
            '$filter': f"receivedDateTime ge {date_filter} and hasAttachments eq true",
            '$orderby': 'receivedDateTime desc',
            '$select': 'id,subject,sender,receivedDateTime,hasAttachments,bodyPreview'
        }
        
        response = requests.get(endpoint, headers=headers, params=params)
        
        if response.status_code == 200:
            return response.json().get('value', [])
        else:
            print(f"Error fetching emails: {response.status_code}")
            print(f"Response: {response.text}")
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
            return response.json().get('value', [])
        else:
            print(f"Error fetching attachments: {response.status_code}")
            print(f"Response: {response.text}")
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
                print("Attachment doesn't contain contentBytes")
                return None
        else:
            print(f"Error downloading attachment: {response.status_code}")
            print(f"Response: {response.text}")
            return None