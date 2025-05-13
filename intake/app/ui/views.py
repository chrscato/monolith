# app/ui/views.py
from flask import Blueprint, render_template

ui_bp = Blueprint("ui", __name__, template_folder="../../templates")

@ui_bp.route("/")
def home():
    # TODO: pull referrals in “pending” state
    return render_template("home.html")
