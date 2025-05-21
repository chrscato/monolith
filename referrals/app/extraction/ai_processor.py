# monolith/referrals/app/extraction/ai_processor.py
"""
AI processing for referral data extraction.
"""
import os
import openai
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class AIExtractor:
    def __init__(self, model="gpt-4", prompt_file=None):
        """Initialize the AI extractor with model and prompt."""
        # Set OpenAI API key
        openai.api_key = os.environ.get('OPENAI_API_KEY')
        
        self.model = model
        
        # Load prompt template from file or use default
        if prompt_file:
            with open(prompt_file, 'r') as f:
                self.prompt_template = f.read()
        else:
            default_prompt_path = Path(__file__).parent / 'default_prompt.txt'
            if default_prompt_path.exists():
                with open(default_prompt_path, 'r') as f:
                    self.prompt_template = f.read()
            else:
                self.prompt_template = """
                You are a medical referral processor. Extract the following information from the referral:
                - Patient's first name
                - Patient's last name
                - Patient's date of birth
                - Patient's phone number
                - Patient's address, city, state, zip
                - Insurance provider
                - Insurance ID
                - Referring physician
                - Physician NPI
                - Service requested
                
                Format your response as JSON.
                
                Referral information:
                {referral_text}
                """
    
    def extract_data(self, referral_text, email_subject=None):
        """
        Extract data from referral text using AI.
        
        Args:
            referral_text: Text content from the referral
            email_subject: Optional email subject for context
            
        Returns:
            dict: Extracted data as a dictionary
        """
        try:
            # Prepare prompt with referral text
            prompt = self.prompt_template.format(
                referral_text=referral_text,
                email_subject=email_subject or ""
            )
            
            # Call OpenAI API
            response = openai.ChatCompletion.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a medical referral processor that extracts structured information."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=1000
            )
            
            # Extract JSON content
            result = response.choices[0].message.content
            
            # Parse result
            import json
            try:
                extracted_data = json.loads(result)
                return extracted_data
            except json.JSONDecodeError:
                logger.error("Failed to parse AI response as JSON")
                # Try to extract JSON from the response using regex
                import re
                json_match = re.search(r'```json\n(.*?)\n```', result, re.DOTALL)
                if json_match:
                    try:
                        extracted_data = json.loads(json_match.group(1))
                        return extracted_data
                    except json.JSONDecodeError:
                        logger.error("Failed to parse JSON from markdown code block")
                        return None
                return None
                
        except Exception as e:
            logger.error(f"Error extracting data with AI: {str(e)}")
            return None