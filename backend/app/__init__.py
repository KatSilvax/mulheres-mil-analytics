# backend/app/__init__.py
from flask import Flask
from flask_cors import CORS
import os

def create_app():
    app = Flask(__name__)
    
    # Habilita CORS para que o frontend possa se comunicar com a API
    CORS(app)
    
    # Configurações básicas
    app.config['SECRET_KEY'] = 'dev-secret-key'  # Troque por uma chave segura em produção!
    app.config['UPLOAD_FOLDER'] = os.path.join(os.path.dirname(__file__), 'static', 'uploads')
    
    # Garante que a pasta de uploads existe
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    
    # Registra as blueprints (rotas)
    from app.routes.data_routes import data_bp
    from app.routes.analysis_routes import analysis_bp
    
    app.register_blueprint(data_bp, url_prefix='/api/data')
    app.register_blueprint(analysis_bp, url_prefix='/api/analysis')
    
    return app