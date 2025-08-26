from flask import Blueprint, request, jsonify
import os
import pandas as pd
import io
from app.services.data_service import process_csv_data, clean_and_standardize_data 

data_bp = Blueprint('data', __name__)

@data_bp.route('/test', methods=['GET'])
def test_connection():
    """Rota simples para testar se a API está funcionando"""
    return jsonify({
        'message': 'Conexão com a API Mulheres Mil estabelecida com sucesso!',
        'status': 'online'
    })

@data_bp.route('/upload', methods=['POST'])
def upload_file():
    """
    Endpoint para upload e processamento do arquivo CSV
    Retorna: Estatísticas básicas e preview dos dados processados
    """
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'Nenhum arquivo enviado'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'Nome de arquivo vazio'}), 400
        
        if file and file.filename.endswith('.csv'):
            # Lê o conteúdo do arquivo
            file_content = file.stream.read().decode('utf-8')
            file.stream.seek(0)  # Reset do ponteiro do arquivo
            
            # Processa o CSV
            result = process_csv_data(file_content)
            
            return jsonify({
                'success': True,
                'message': 'Arquivo processado com sucesso',
                'data': result
            })
            
        else:
            return jsonify({'error': 'Apenas arquivos CSV são permitidos'}), 400
            
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Erro no processamento do arquivo: {str(e)}'
        }), 500

@data_bp.route('/basic-stats', methods=['POST'])
def get_basic_stats():
    """
    Endpoint alternativo: envia dados CSV via JSON para análise
    Útil para desenvolvimento e teste
    """
    try:
        data = request.get_json()
        if not data or 'csv_content' not in data:
            return jsonify({'error': 'Conteúdo CSV não fornecido'}), 400
        
        result = process_csv_data(data['csv_content'])
        return jsonify({'success': True, 'data': result})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500