# monolith/referrals/scripts/process_queue.py
"""
Script to process the referral queue with AI extraction.
"""
import os
import sys
from pathlib import Path
import logging
from datetime import datetime

# Add parent directory to Python path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))

from app.queue.manager import QueueManager
from app.extraction.ai_processor import AIExtractor
from app.file_storage.s3_storage import S3Storage
from models.models import Referral, Attachment, ExtractedData
from models.database import get_session, init_db

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Path(__file__).parent / 'process_queue.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def process_queue(batch_size=5):
    """
    Process the referral queue with AI extraction.
    
    Args:
        batch_size: Number of referrals to process in one batch
    """
    # Initialize clients
    ai_extractor = AIExtractor()
    s3_storage = S3Storage()
    
    # Get pending extractions
    referrals = QueueManager.get_pending_extraction(limit=batch_size)
    logger.info(f"Found {len(referrals)} referrals pending extraction")
    
    # Process each referral
    for referral in referrals:
        logger.info(f"Processing referral {referral.id}")
        session = get_session()
        
        try:
            # Get attachments
            attachments = session.query(Attachment).filter(
                Attachment.referral_id == referral.id
            ).all()
            
            # Prepare text for extraction
            extraction_text = f"Subject: {referral.subject}\n\n"
            
            if referral.body_text:
                extraction_text += f"Email Body:\n{referral.body_text}\n\n"
            
            # Download and process text from attachments
            for attachment in attachments:
                if attachment.uploaded and attachment.content_type.startswith(('text/', 'application/pdf')):
                    file_content = s3_storage.download_file(attachment.s3_key)
                    
                    if file_content:
                        # For PDF files, extract text
                        if attachment.content_type == 'application/pdf':
                            try:
                                import PyPDF2
                                from io import BytesIO
                                
                                pdf_file = BytesIO(file_content)
                                pdf_reader = PyPDF2.PdfReader(pdf_file)
                                
                                text = ""
                                for page_num in range(len(pdf_reader.pages)):
                                    text += pdf_reader.pages[page_num].extract_text()
                                
                                extraction_text += f"Attachment: {attachment.filename}\n{text}\n\n"
                            except Exception as e:
                                logger.error(f"Error extracting PDF text: {str(e)}")
                        else:
                            # For text files, use content directly
                            try:
                                text = file_content.decode('utf-8')
                                extraction_text += f"Attachment: {attachment.filename}\n{text}\n\n"
                            except UnicodeDecodeError:
                                logger.error(f"Error decoding attachment as UTF-8")
            
            # Extract data using AI
            extracted_data = ai_extractor.extract_data(extraction_text, referral.subject)
            
            if extracted_data:
                # Store extracted data
                data_record = session.query(ExtractedData).filter(
                    ExtractedData.referral_id == referral.id
                ).first()
                
                if not data_record:
                    data_record = ExtractedData(referral_id=referral.id)
                    session.add(data_record)
                
                # Update fields from extraction
                data_record.patient_first_name = extracted_data.get('patient_first_name', '')
                data_record.patient_last_name = extracted_data.get('patient_last_name', '')
                data_record.patient_dob = extracted_data.get('patient_dob', '')
                data_record.patient_phone = extracted_data.get('patient_phone', '')
                data_record.patient_address = extracted_data.get('patient_address', '')
                data_record.patient_city = extracted_data.get('patient_city', '')
                data_record.patient_state = extracted_data.get('patient_state', '')
                data_record.patient_zip = extracted_data.get('patient_zip', '')
                data_record.insurance_provider = extracted_data.get('insurance_provider', '')
                data_record.insurance_id = extracted_data.get('insurance_id', '')
                data_record.referring_physician = extracted_data.get('referring_physician', '')
                data_record.physician_npi = extracted_data.get('physician_npi', '')
                data_record.service_requested = extracted_data.get('service_requested', '')
                data_record.status = 'extracted'
                data_record.updated_at = datetime.utcnow()
                
                # Update referral status
                referral.updated_at = datetime.utcnow()
                
                session.commit()
                logger.info(f"Successfully extracted data for referral {referral.id}")
            else:
                logger.error(f"Failed to extract data for referral {referral.id}")
                
        except Exception as e:
            session.rollback()
            logger.error(f"Error processing referral {referral.id}: {str(e)}")
        finally:
            session.close()

if __name__ == "__main__":
    # Ensure database is initialized
    init_db()
    
    # Process the queue
    process_queue()