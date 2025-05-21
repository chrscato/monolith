# monolith/referrals/models/models.py
"""
Define SQLAlchemy models for the referrals database
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import datetime

Base = declarative_base()

class Referral(Base):
    __tablename__ = 'referrals'
    
    id = Column(Integer, primary_key=True)
    email_id = Column(String(100), unique=True)
    subject = Column(String(255))
    sender = Column(String(100))
    received_date = Column(DateTime)
    body_text = Column(Text)
    status = Column(String(50), default='new')  # new, processing, reviewed, completed
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    
    attachments = relationship("Attachment", back_populates="referral")
    extracted_data = relationship("ExtractedData", uselist=False, back_populates="referral")
    
class Attachment(Base):
    __tablename__ = 'attachments'
    
    id = Column(Integer, primary_key=True)
    referral_id = Column(Integer, ForeignKey('referrals.id'))
    filename = Column(String(255))
    s3_key = Column(String(255))
    content_type = Column(String(100))
    size = Column(Integer)
    uploaded = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    
    referral = relationship("Referral", back_populates="attachments")

class ExtractedData(Base):
    __tablename__ = 'extracted_data'
    
    id = Column(Integer, primary_key=True)
    referral_id = Column(Integer, ForeignKey('referrals.id'))
    patient_first_name = Column(String(100))
    patient_last_name = Column(String(100))
    patient_dob = Column(String(20))
    patient_phone = Column(String(20))
    patient_address = Column(Text)
    patient_city = Column(String(100))
    patient_state = Column(String(2))
    patient_zip = Column(String(10))
    insurance_provider = Column(String(100))
    insurance_id = Column(String(50))
    referring_physician = Column(String(100))
    physician_npi = Column(String(20))
    service_requested = Column(String(255))
    status = Column(String(50), default='pending')  # pending, verified, invalid
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    
    referral = relationship("Referral", back_populates="extracted_data")