import os
from pathlib import Path
from flask import Flask, render_template, jsonify, request, redirect, url_for
from database import db, FailedBill, LineItem

# Set up paths
BASE_DIR = Path(__file__).resolve().parent
INSTANCE_PATH = BASE_DIR / 'instance'
INSTANCE_PATH.mkdir(exist_ok=True)

# Create Flask app
app = Flask(__name__, instance_path=str(INSTANCE_PATH))

# Configure app
class Config:
    """Base configuration."""
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev')
    SQLALCHEMY_TRACK_MODIFICATIONS = False

class DevelopmentConfig(Config):
    """Development configuration."""
    SQLALCHEMY_DATABASE_URI = f'sqlite:///{INSTANCE_PATH}/billing_failures_dev.db'
    DEBUG = True

class TestingConfig(Config):
    """Testing configuration."""
    SQLALCHEMY_DATABASE_URI = f'sqlite:///{INSTANCE_PATH}/billing_failures_test.db'
    TESTING = True

class ProductionConfig(Config):
    """Production configuration."""
    SQLALCHEMY_DATABASE_URI = f'sqlite:///{INSTANCE_PATH}/billing_failures.db'

# Set configuration
config = {
    'development': DevelopmentConfig,
    'testing': TestingConfig,
    'production': ProductionConfig,
    'default': DevelopmentConfig
}

# Configure app based on environment
env = os.environ.get('FLASK_ENV', 'development')
app.config.from_object(config[env])

# Initialize database
db.init_app(app)

# Create tables
with app.app_context():
    db.create_all()
    print(f"Using database: {app.config['SQLALCHEMY_DATABASE_URI']}")

# Routes
@app.route('/')
def index():
    """List all failed bills."""
    bills = FailedBill.query.all()
    return render_template('index.html', bills=bills)

@app.route('/bill/<bill_id>')
def view_bill(bill_id):
    """View a specific failed bill."""
    bill = FailedBill.query.filter_by(bill_id=bill_id).first_or_404()
    return render_template('bill.html', bill=bill)

@app.route('/bill/<bill_id>/update', methods=['POST'])
def update_bill(bill_id):
    """Update bill status."""
    bill = FailedBill.query.filter_by(bill_id=bill_id).first_or_404()
    bill.status = request.form.get('status', 'pending')
    db.session.commit()
    return redirect(url_for('view_bill', bill_id=bill_id))

@app.route('/line_item/<int:item_id>/update', methods=['POST'])
def update_line_item(item_id):
    """Update line item status."""
    item = LineItem.query.get_or_404(item_id)
    item.status = request.form.get('status', 'pending')
    db.session.commit()
    return redirect(url_for('view_bill', bill_id=item.bill_id))

if __name__ == '__main__':
    app.run(debug=True) 