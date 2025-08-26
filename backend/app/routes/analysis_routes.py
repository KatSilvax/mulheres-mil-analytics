from flask import Blueprint, jsonify
from app.services.analysis_service import generate_detailed_analysis

analysis_bp = Blueprint('analysis', __name__)

@analysis_bp.route('/detailed', methods=['POST'])
def get_detailed_analysis():
    """Retorna análise detalhada dos dados"""
    # Implementar similar ao upload, mas com análise mais profunda
    pass