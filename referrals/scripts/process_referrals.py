# referrals/scripts/process_referrals.py
"""
Complete referrals processing workflow:
1. Fetch emails from Outlook
2. Extract data with AI
3. Mark for review
"""
import logging
import time
from pathlib import Path
from email_fetcher import ClarityEmailFetcher
from ai_extractor import WorkersCompAIExtractor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('referrals_processing.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class ReferralsProcessor:
    def __init__(self):
        """Initialize the complete processing pipeline."""
        self.email_fetcher = ClarityEmailFetcher()
        self.ai_extractor = WorkersCompAIExtractor()
    
    def run_full_pipeline(self, days=1, max_emails=50, extract_limit=20):
        """Run the complete referrals processing pipeline."""
        logger.info("🚀 Starting referrals processing pipeline")
        
        try:
            # Step 1: Fetch new emails
            logger.info("📧 Step 1: Fetching emails from Outlook")
            self.email_fetcher.process_emails(days=days, max_emails=max_emails)
            
            # Small delay to ensure database writes are complete
            time.sleep(2)
            
            # Step 2: Extract data with AI
            logger.info("🤖 Step 2: Extracting data with AI")
            self.ai_extractor.process_extractions(limit=extract_limit)
            
            logger.info("✅ Pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {str(e)}")
            raise
    
    def run_email_only(self, days=1, max_emails=50):
        """Run only email fetching."""
        logger.info("📧 Running email fetching only")
        self.email_fetcher.process_emails(days=days, max_emails=max_emails)
    
    def run_extraction_only(self, limit=20):
        """Run only AI extraction."""
        logger.info("🤖 Running AI extraction only")
        self.ai_extractor.process_extractions(limit=limit)

if __name__ == "__main__":
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process workers comp referrals')
    parser.add_argument('--mode', choices=['full', 'email', 'extract'], default='full',
                       help='Processing mode (default: full)')
    parser.add_argument('--days', type=int, default=1, 
                       help='Days to look back for emails (default: 1)')
    parser.add_argument('--max-emails', type=int, default=50,
                       help='Max emails to fetch (default: 50)')
    parser.add_argument('--extract-limit', type=int, default=20,
                       help='Max emails to extract (default: 20)')
    
    args = parser.parse_args()
    
    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()
    
    # Initialize processor
    processor = ReferralsProcessor()
    
    # Run based on mode
    if args.mode == 'full':
        print(f"🔄 Running full pipeline (emails: {args.days} days, extraction: {args.extract_limit})")
        processor.run_full_pipeline(
            days=args.days, 
            max_emails=args.max_emails, 
            extract_limit=args.extract_limit
        )
    elif args.mode == 'email':
        print(f"📧 Running email fetching only ({args.days} days)")
        processor.run_email_only(days=args.days, max_emails=args.max_emails)
    elif args.mode == 'extract':
        print(f"🤖 Running AI extraction only ({args.extract_limit} emails)")
        processor.run_extraction_only(limit=args.extract_limit)