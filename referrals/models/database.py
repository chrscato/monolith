# monolith/referrals/models/database.py
"""
Database connection and initialization
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pathlib import Path
import os

from .models import Base

# Get the database file path
DB_FILE = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) / "referrals.db"

def get_engine(db_path=DB_FILE):
    """Get SQLAlchemy engine for the database."""
    return create_engine(f"sqlite:///{db_path}")

def init_db():
    """Initialize the database with tables."""
    engine = get_engine()
    Base.metadata.create_all(engine)
    return engine

def get_session():
    """Get a database session."""
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()