# referrals/scripts/ai_extractor.py
"""
AI-powered data extraction for workers' compensation referrals.
Processes emails and attachments to extract structured data.
"""
import os
import sqlite3
import openai
import json
import boto3
import PyPDF2
import logging
from io import BytesIO
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WorkersCompAIExtractor:
    def __init__(self):
        """Initialize the AI extractor with OpenAI and AWS clients."""
        # Set OpenAI API key
        openai.api_key = os.environ.get('OPENAI_API_KEY')
        
        self.db_path = os.environ.get('REFERRALS_DB', 'referrals_wc.db')
        self.s3_bucket = os.environ.get('S3_BUCKET')
        
        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
            region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-2')
        )
        
        # Load the extraction prompt from file
        prompt_file = Path(__file__).parent / 'prompts' / 'workers_comp_extraction.txt'
        if prompt_file.exists():
            with open(prompt_file, 'r') as f:
                self.extraction_prompt = f.read()
        else:
            logger.warning(f"Prompt file not found at {prompt_file}, using default")
            self.extraction_prompt = self._get_default_prompt()
    
    def get_pending_extractions(self, limit=10):
        """Get emails that need AI extraction with complete metadata."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Get emails that don't have referral records yet - with full metadata
            cursor.execute('''
                SELECT om.id, om.message_id, om.subject, om.sender_email, om.sender_name,
                       om.received_datetime, om.sent_datetime, om.body_preview, om.body_content,
                       om.importance, om.web_link
                FROM outlook_messages om
                LEFT JOIN referrals r ON om.id = r.message_id
                WHERE om.processing_status = 'processed' 
                AND r.id IS NULL
                ORDER BY om.received_datetime DESC
                LIMIT ?
            ''', (limit,))
            
            emails = cursor.fetchall()
            logger.info(f"Found {len(emails)} emails pending extraction")
            return emails
            
        finally:
            conn.close()
    
    def extract_pdf_text(self, s3_key):
        """Extract text from PDF file stored in S3."""
        try:
            # Download PDF from S3
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
            pdf_content = response['Body'].read()
            
            # Extract text using PyPDF2
            pdf_file = BytesIO(pdf_content)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            text = ""
            for page in pdf_reader.pages:
                text += page.extract_text() + "\n"
            
            return text.strip()
            
        except Exception as e:
            logger.error(f"Error extracting PDF text from {s3_key}: {str(e)}")
            return ""
    
    def get_attachments_text(self, message_db_id):
        """Get text content from all attachments for an email."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT filename, content_type, s3_key
                FROM attachments
                WHERE message_id = ? AND upload_status = 'uploaded'
            ''', (message_db_id,))
            
            attachments = cursor.fetchall()
            attachment_texts = []
            
            for filename, content_type, s3_key in attachments:
                if content_type == 'application/pdf' and s3_key:
                    text = self.extract_pdf_text(s3_key)
                    if text:
                        attachment_texts.append(f"=== {filename} ===\n{text}")
                elif content_type.startswith('text/') and s3_key:
                    try:
                        # Download text file from S3
                        response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
                        text = response['Body'].read().decode('utf-8')
                        attachment_texts.append(f"=== {filename} ===\n{text}")
                    except Exception as e:
                        logger.error(f"Error reading text file {filename}: {str(e)}")
            
            return "\n\n".join(attachment_texts)
            
        finally:
            conn.close()
    
    def extract_data_with_ai(self, email_metadata, attachments_content):
        """Use OpenAI to extract structured data from email and attachments."""
        try:
            # Build comprehensive email context
            email_context = f"""
EMAIL METADATA:
Message ID: {email_metadata.get('message_id', '')}
Subject: {email_metadata.get('subject', '')}
From: {email_metadata.get('sender_name', '')} <{email_metadata.get('sender_email', '')}>
Received: {email_metadata.get('received_datetime', '')}
Sent: {email_metadata.get('sent_datetime', '')}
Importance: {email_metadata.get('importance', '')}

EMAIL BODY:
{email_metadata.get('body_content') or email_metadata.get('body_preview', '')}

ATTACHMENTS CONTENT:
{attachments_content}
"""
            
            # Call OpenAI API with proper message structure
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[
                    {
                        "role": "system", 
                        "content": self.extraction_prompt
                    },
                    {
                        "role": "user", 
                        "content": f"Please extract workers' compensation referral data from the following:\n\n{email_context}"
                    }
                ],
                temperature=0,
                max_tokens=2000
            )
            
            # Parse JSON response
            result = response.choices[0].message.content.strip()
            
            # Clean up response (remove markdown code blocks if present)
            if result.startswith('```json'):
                result = result[7:]
            if result.endswith('```'):
                result = result[:-3]
            
            result = result.strip()
            
            extracted_data = json.loads(result)
            logger.info("Successfully extracted data with AI")
            return extracted_data
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse AI response as JSON: {str(e)}")
            logger.error(f"AI Response: {result}")
            return None
        except Exception as e:
            logger.error(f"Error in AI extraction: {str(e)}")
            return None
    
    def _get_default_prompt(self):
        """Default prompt if file is not found."""
        return """You are a workers' compensation referral intake specialist. Extract data from the referral and return only valid JSON with the required fields. Use null for missing information."""
    
    def save_referral_data(self, message_db_id, extracted_data):
        """Save extracted data to the referrals table."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Generate referral number
            now = datetime.now()
            referral_number = f"REF-{now.strftime('%Y%m%d')}-{message_db_id:04d}"
            
            # Insert referral data
            cursor.execute('''
                INSERT INTO referrals (
                    message_id, referral_number,
                    iw_first_name, iw_last_name, iw_date_of_birth, iw_phone, iw_email,
                    iw_address, iw_city, iw_state, iw_zip_code, iw_employee_id, iw_job_title,
                    employer_name, employer_phone, employer_address, employer_contact_name,
                    claim_number, adjuster_name, adjuster_phone, adjuster_email, insurance_carrier,
                    date_of_injury, injury_description, body_parts_affected,
                    service_type, cpt_codes, icd10_codes, service_frequency, authorized_visits, authorization_number,
                    referring_provider_name, referring_provider_npi, referring_provider_phone,
                    service_provider_name, service_provider_npi, service_provider_phone,
                    diagnosis_primary, clinical_notes, treatment_goals, work_restrictions,
                    priority_level, notes, referral_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                message_db_id, referral_number,
                extracted_data.get('iw_first_name'), extracted_data.get('iw_last_name'), 
                extracted_data.get('iw_date_of_birth'), extracted_data.get('iw_phone'), 
                extracted_data.get('iw_email'), extracted_data.get('iw_address'),
                extracted_data.get('iw_city'), extracted_data.get('iw_state'), 
                extracted_data.get('iw_zip_code'), extracted_data.get('iw_employee_id'),
                extracted_data.get('iw_job_title'), extracted_data.get('employer_name'),
                extracted_data.get('employer_phone'), extracted_data.get('employer_address'),
                extracted_data.get('employer_contact_name'), extracted_data.get('claim_number'),
                extracted_data.get('adjuster_name'), extracted_data.get('adjuster_phone'),
                extracted_data.get('adjuster_email'), extracted_data.get('insurance_carrier'),
                extracted_data.get('date_of_injury'), extracted_data.get('injury_description'),
                extracted_data.get('body_parts_affected'), extracted_data.get('service_type'),
                extracted_data.get('cpt_codes'), extracted_data.get('icd10_codes'),
                extracted_data.get('service_frequency'), extracted_data.get('authorized_visits'),
                extracted_data.get('authorization_number'), extracted_data.get('referring_provider_name'),
                extracted_data.get('referring_provider_npi'), extracted_data.get('referring_provider_phone'),
                extracted_data.get('service_provider_name'), extracted_data.get('service_provider_npi'),
                extracted_data.get('service_provider_phone'), extracted_data.get('diagnosis_primary'),
                extracted_data.get('clinical_notes'), extracted_data.get('treatment_goals'),
                extracted_data.get('work_restrictions'), extracted_data.get('priority_level', 'routine'),
                extracted_data.get('notes'), 'extracted'
            ))
            
            referral_id = cursor.lastrowid
            
            # Update email processing status
            cursor.execute('''
                UPDATE outlook_messages 
                SET processing_status = 'extracted' 
                WHERE id = ?
            ''', (message_db_id,))
            
            conn.commit()
            logger.info(f"Saved referral {referral_number} with ID {referral_id}")
            return referral_id
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error saving referral data: {str(e)}")
            return None
        finally:
            conn.close()
    
    def process_extractions(self, limit=10):
        """Main method to process pending extractions."""
        logger.info("Starting AI extraction process")
        
        # Get pending emails
        pending_emails = self.get_pending_extractions(limit)
        
        for email in pending_emails:
            (message_db_id, message_id, subject, sender_email, sender_name,
             received_datetime, sent_datetime, body_preview, body_content,
             importance, web_link) = email
            
            logger.info(f"Processing email: {subject[:50]}...")
            
            # Prepare complete email metadata
            email_metadata = {
                'message_id': message_id,
                'subject': subject,
                'sender_email': sender_email,
                'sender_name': sender_name,
                'received_datetime': received_datetime,
                'sent_datetime': sent_datetime,
                'body_content': body_content,
                'body_preview': body_preview,
                'importance': importance,
                'web_link': web_link
            }
            
            # Get attachment text
            attachments_content = self.get_attachments_text(message_db_id)
            
            # Extract data with AI
            extracted_data = self.extract_data_with_ai(email_metadata, attachments_content)
            
            if extracted_data:
                # Save to database
                referral_id = self.save_referral_data(message_db_id, extracted_data)
                if referral_id:
                    logger.info(f"✅ Successfully processed email {message_id}")
                else:
                    logger.error(f"❌ Failed to save referral data for {message_id}")
            else:
                logger.error(f"❌ Failed to extract data for {message_id}")
                
        logger.info("AI extraction process completed")

if __name__ == "__main__":
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Extract data from referral emails using AI')
    parser.add_argument('--limit', type=int, default=10, help='Max emails to process (default: 10)')
    parser.add_argument('--test-mode', action='store_true', help='Test mode with verbose output')
    
    args = parser.parse_args()
    
    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()
    
    # Process extractions
    extractor = WorkersCompAIExtractor()
    
    if args.test_mode:
        print("🧪 TEST MODE: Verbose output enabled")
        logging.getLogger().setLevel(logging.DEBUG)
    
    print(f"🤖 Processing up to {args.limit} emails for AI extraction")
    extractor.process_extractions(limit=args.limit)