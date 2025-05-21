# monolith/referrals/app/queue/manager.py
"""
Queue management for referral processing.
"""
import logging
from sqlalchemy import and_
from datetime import datetime

# Import models
from ...models.models import Referral, Attachment, ExtractedData
from ...models.database import get_session

logger = logging.getLogger(__name__)

class QueueManager:
    """Manager for the referrals processing queue."""
    
    @staticmethod
    def get_pending_referrals(limit=10):
        """
        Get pending referrals for processing.
        
        Args:
            limit: Maximum number of referrals to return
            
        Returns:
            list: List of Referral objects
        """
        session = get_session()
        try:
            referrals = session.query(Referral).filter(
                Referral.status == 'new'
            ).order_by(
                Referral.received_date
            ).limit(limit).all()
            
            return referrals
        finally:
            session.close()
    
    @staticmethod
    def get_pending_extraction(limit=10):
        """
        Get referrals with files uploaded but not yet processed by AI.
        
        Args:
            limit: Maximum number of referrals to return
            
        Returns:
            list: List of Referral objects
        """
        session = get_session()
        try:
            # Find referrals with status 'processing' 
            # that don't have extracted data or have status 'pending'
            referrals = session.query(Referral).outerjoin(
                ExtractedData
            ).filter(
                and_(
                    Referral.status == 'processing',
                    (ExtractedData.id == None) | (ExtractedData.status == 'pending')
                )
            ).order_by(
                Referral.received_date
            ).limit(limit).all()
            
            return referrals
        finally:
            session.close()
    
    @staticmethod
    def get_pending_review(limit=10):
        """
        Get referrals with extracted data that need review.
        
        Args:
            limit: Maximum number of referrals to return
            
        Returns:
            list: List of Referral objects with extracted data
        """
        session = get_session()
        try:
            referrals = session.query(Referral).join(
                ExtractedData
            ).filter(
                and_(
                    Referral.status == 'processing',
                    ExtractedData.status == 'extracted'
                )
            ).order_by(
                Referral.received_date
            ).limit(limit).all()
            
            return referrals
        finally:
            session.close()
    
    @staticmethod
    def update_referral_status(referral_id, status, error=None):
        """
        Update the status of a referral.
        
        Args:
            referral_id: ID of the referral
            status: New status
            error: Optional error message
            
        Returns:
            bool: True if successful, False otherwise
        """
        session = get_session()
        try:
            referral = session.query(Referral).filter(Referral.id == referral_id).first()
            if not referral:
                logger.error(f"Referral {referral_id} not found")
                return False
            
            referral.status = status
            referral.updated_at = datetime.utcnow()
            
            if error:
                # If there's a way to store error messages in your schema
                referral.last_error = error
            
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            logger.error(f"Error updating referral status: {str(e)}")
            return False
        finally:
            session.close()