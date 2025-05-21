# monolith/referrals/scripts/fetch_emails.py
"""
Script to fetch emails from shared inbox and create referrals.
"""
import os
import sys
from pathlib import Path
import logging
from datetime import datetime
import tempfile

# Add parent directory to Python path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))

from app.email_fetcher.graph_client import GraphAPIClient
from app.file_storage.s3_storage import S3Storage
from models.models import Referral, Attachment
from models.database import get_session, init_db

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Path(__file__).parent / 'fetch_emails.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def process_emails(days=1, max_emails=20):
    """
    Process emails from shared inbox.
    
    Args:
        days: Look back days
        max_emails: Maximum emails to process
    """
    # Initialize clients
    graph_client = GraphAPIClient()
    s3_storage = S3Storage()
    
    # Get unprocessed emails
    emails = graph_client.get_unprocessed_emails(days=days, max_emails=max_emails)
    logger.info(f"Found {len(emails)} unprocessed emails")
    
    # Process each email
    for email in emails:
        email_id = email.get('id')
        
        # Check if email already exists in database
        session = get_session()
        existing = session.query(Referral).filter(Referral.email_id == email_id).first()
        
        if existing:
            logger.info(f"Email {email_id} already processed, skipping")
            session.close()
            continue
        
        # Create new referral
        try:
            referral = Referral(
                email_id=email_id,
                subject=email.get('subject', ''),
                sender=email.get('sender', {}).get('emailAddress', {}).get('address', ''),
                received_date=datetime.fromisoformat(email.get('receivedDateTime', '').replace('Z', '+00:00')),
                body_text=email.get('bodyPreview', ''),
                status='new'
            )
            
            session.add(referral)
            session.commit()
            
            logger.info(f"Created referral for email {email_id}")
            
            # Get attachments
            attachments = graph_client.get_email_attachments(email_id)
            logger.info(f"Found {len(attachments)} attachments")
            
            # Process each attachment
            for attachment_data in attachments:
                attachment_id = attachment_data.get('id')
                filename = attachment_data.get('name', '')
                
                # Download attachment
                file_content = graph_client.download_attachment(email_id, attachment_id)
                
                if file_content:
                    # Create a temporary file to determine content type
                    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                        temp_file.write(file_content)
                        temp_path = temp_file.name
                    
                    # Determine content type
                    import mimetypes
                    content_type = attachment_data.get('contentType') or mimetypes.guess_type(filename)[0] or 'application/octet-stream'
                    
                    # Create S3 key - use referral ID and filename
                    s3_key = f"referrals/{referral.id}/{filename}"
                    
                    # Upload to S3
                    if s3_storage.upload_file(file_content, s3_key, content_type):
                        # Create attachment record
                        db_attachment = Attachment(
                            referral_id=referral.id,
                            filename=filename,
                            s3_key=s3_key,
                            content_type=content_type,
                            size=len(file_content),
                            uploaded=True
                        )
                        
                        session.add(db_attachment)
                        logger.info(f"Uploaded attachment {filename} to S3")
                    else:
                        logger.error(f"Failed to upload attachment {filename} to S3")
                    
                    # Remove temp file
                    os.unlink(temp_path)
                else:
                    logger.error(f"Failed to download attachment {attachment_id}")
            
            # Update referral status
            referral.status = 'processing'
            session.commit()
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error processing email {email_id}: {str(e)}")
        finally:
            session.close()

if __name__ == "__main__":
    # Ensure database is initialized
    init_db()
    
    # Process emails
    process_emails()