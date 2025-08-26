# backend/app/services/analysis_service.py
import pandas as pd
from typing import Dict, Any

def generate_detailed_analysis(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Gera análise detalhada dos dados padronizados
    """
    analysis = {}
    
    # 1. Análise Demográfica
    analysis['demografia'] = {
        'faixa_etaria': df['faixa_etaria'].value_counts().to_dict(),
        'escolaridade': df['escolaridade_simplificada'].value_counts().to_dict(),
        'renda': df['renda_categoria'].value_counts().to_dict(),
        'total_respondentes': len(df)
    }
    
    # 2. Análise de Avaliações (Médias e distribuição)
    analysis['avaliacoes'] = {
        'coordenador': {
            'media': round(df['avaliacao_coordenador_score'].mean(), 2),
            'distribuicao': df['avaliacao_coordenador'].value_counts().to_dict()
        },
        'assistente': {
            'media': round(df['avaliacao_assistente_score'].mean(), 2),
            'distribuicao': df['avaliacao_assistente'].value_counts().to_dict()
        },
        'aulas': {
            'media': round(df['avaliacao_aulas_score'].mean(), 2),
            'distribuicao': df['avaliacao_aulas'].value_counts().to_dict()
        }
    }
    
    # 3. Análise de Desafios (palavras mais frequentes)
    todos_desafios = ' '.join(df['desafios'].dropna().astype(str)).lower()
    palavras_chave = ['transporte', 'distancia', 'tempo', 'filhos', 'material', 'lanche', 'horario']
    
    analysis['desafios'] = {
        'principais_temas': {
            palavra: todos_desafios.count(palavra) 
            for palavra in palavras_chave 
            if todos_desafios.count(palavra) > 0
        }
    }
    
    # 4. Análise de Cursos Desejados
    analysis['proximos_cursos'] = {
        'mais_solicitados': df['proximo_curso'].value_counts().head(10).to_dict()
    }
    
    # 5. Métricas de Qualidade
    analysis['metricas'] = {
        'taxa_resposta': round((len(df) / len(df)) * 100, 1),  # Todos responderam
        'dados_inconsistentes': {
            'faixa_etaria_nao_informada': (df['faixa_etaria'] == 'Nao informado').sum(),
            'renda_nao_informada': (df['renda_categoria'] == 'Nao informado').sum()
        }
    }
    
    return analysis