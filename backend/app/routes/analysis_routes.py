from flask import Blueprint, request, jsonify
import pandas as pd
import io
from app.services.data_service import clean_and_standardize_data
from app.services.analysis_service import generate_detailed_analysis

analysis_bp = Blueprint('analysis', __name__)

@analysis_bp.route('/detailed', methods=['POST'])
def get_detailed_analysis():
    """
    Endpoint para análise detalhada dos dados
    """
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'Nenhum arquivo enviado'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'Nome de arquivo vazio'}), 400
        
        if file and file.filename.endswith('.csv'):
            # Lê e processa o CSV
            file_content = file.stream.read().decode('utf-8')
            df = pd.read_csv(io.StringIO(file_content))
            
            # Limpa e padroniza
            df_clean, _ = clean_and_standardize_data(df)
            
            # Gera análise detalhada
            analysis = generate_detailed_analysis(df_clean)
            
            return jsonify({
                'success': True,
                'analysis': analysis
            })
            
        else:
            return jsonify({'error': 'Apenas arquivos CSV são permitidos'}), 400
            
    except Exception as e:
        return jsonify({'error': f'Erro na análise: {str(e)}'}), 500

@analysis_bp.route('/test', methods=['GET'])
def test_analysis():
    """Teste simples da rota de análise"""
    return jsonify({'message': 'Rota de análise funcionando!'})