from datetime import datetime
from app import db

class FailedBill(db.Model):
    """Model for failed bills that need review."""
    __tablename__ = 'failed_bills'
    
    id = db.Column(db.Integer, primary_key=True)
    bill_id = db.Column(db.String(255), unique=True, nullable=False)
    claim_id = db.Column(db.String(255), nullable=False)
    patient_name = db.Column(db.String(255), nullable=False)
    patient_dob = db.Column(db.String(255))
    provider_name = db.Column(db.String(255))
    provider_npi = db.Column(db.String(255))
    total_charge = db.Column(db.Float)
    failure_type = db.Column(db.String(50), nullable=False)
    failure_details = db.Column(db.Text)
    status = db.Column(db.String(50), default='pending')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    line_items = db.relationship('LineItem', backref='bill', lazy=True)

class LineItem(db.Model):
    """Model for bill line items."""
    __tablename__ = 'line_items'
    
    id = db.Column(db.Integer, primary_key=True)
    bill_id = db.Column(db.String(255), db.ForeignKey('failed_bills.bill_id'), nullable=False)
    line_item_id = db.Column(db.String(255), nullable=False)
    cpt_code = db.Column(db.String(50))
    charge_amount = db.Column(db.Float)
    date_of_service = db.Column(db.String(255))
    status = db.Column(db.String(50), default='pending')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow) 