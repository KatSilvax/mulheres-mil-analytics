# backend/app/routes/data_routes.py
from flask import Blueprint, jsonify

data_bp = Blueprint('data', __name__)

@data_bp.route('/test', methods=['GET'])
def test_connection():
    """Rota simples para testar se a API está funcionando"""
    return jsonify({
        'message': 'Conexão com a API Mulheres Mil estabelecida com sucesso!',
        'status': 'online'
    })