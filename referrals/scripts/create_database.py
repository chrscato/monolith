# monolith/referrals/scripts/create_database.py
"""
Script to create the referrals database.
"""
import os
import sys
from pathlib import Path

# Add parent directory to Python path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))

from models.database import init_db

def create_database():
    """Create the referrals database."""
    print("Creating referrals database...")
    engine = init_db()
    print(f"Database created successfully at {engine.url}")

if __name__ == "__main__":
    create_database()