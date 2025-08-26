# backend/app/services/data_service.py
import pandas as pd
import io
import logging
from typing import Dict, Any, Tuple
import re

# Configura logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_csv_data(file_content: str) -> Dict[str, Any]:
    """
    Processa o conteúdo CSV e retorna estatísticas básicas
    """
    try:
        # Lê o CSV diretamente do conteúdo string
        df = pd.read_csv(io.StringIO(file_content))
        
        logger.info(f"CSV lido com sucesso. Shape: {df.shape}")
        
        # Limpeza e padronização básica
        df_clean, logs = clean_and_standardize_data(df)
        
        # Gera estatísticas básicas
        stats = generate_basic_stats(df_clean)
        
        # Preview dos dados (apenas primeiras 5 linhas)
        preview = df_clean.head().fillna('').to_dict(orient='records')
        
        return {
            'shape': {'rows': df.shape[0], 'columns': df.shape[1]},
            'columns': list(df_clean.columns),
            'column_types': get_column_types(df_clean),
            'basic_stats': stats,
            'cleaning_logs': logs,
            'preview': preview
        }
        
    except Exception as e:
        logger.error(f"Erro ao processar CSV: {str(e)}")
        raise Exception(f'Erro ao processar CSV: {str(e)}')

def clean_and_standardize_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, list]:
    """
    Primeira etapa de limpeza e padronização dos dados
    Retorna: DataFrame limpo e lista de logs das alterações
    """
    logs = []
    df_clean = df.copy()
    
    # 1. Remove espaços extras dos nomes das colunas
    original_columns = list(df_clean.columns)
    df_clean.columns = df_clean.columns.str.strip()
    
    if original_columns != list(df_clean.columns):
        logs.append("Espaços removidos dos nomes das colunas")
    
    # 2. Remove linhas completamente vazias
    initial_rows = len(df_clean)
    df_clean = df_clean.dropna(how='all')
    if len(df_clean) < initial_rows:
        removed = initial_rows - len(df_clean)
        logs.append(f"Removidas {removed} linha(s) completamente vazia(s)")
    
    # 3. Preenche valores NaN com string vazia para colunas de texto
    text_columns = df_clean.select_dtypes(include=['object']).columns
    df_clean[text_columns] = df_clean[text_columns].fillna('')
    logs.append("Valores vazios preenchidos com string vazia")
    
    # 4. Aplica padronizações específicas do projeto Mulheres Mil
    df_clean = standardize_specific_columns(df_clean, logs)
    
    # 5. Log de colunas identificadas
    logs.append(f"Colunas finais: {', '.join(df_clean.columns)}")
    
    return df_clean, logs

def standardize_specific_columns(df: pd.DataFrame, logs: list) -> pd.DataFrame:
    """
    Padronizações ESPECÍFICAS para os dados do Mulheres Mil
    """
    df_clean = df.copy()
    
    # 1. Mapeamento de colunas (nomes em português claro)
    column_mapping = {
        'Carimbo de data/hora': 'timestamp',
        'Nome do Curso:': 'curso',
        'Instituição Ofertante:': 'instituicao', 
        'Faixa etária predominante': 'faixa_etaria',
        'Profissão:': 'profissao',
        'Situação Socioeconômica: Renda per capta:': 'renda_original',
        'Escolaridade:': 'escolaridade',
        'Motivação para ingresso no curso:': 'motivacao',
        'Desafios enfrentados:': 'desafios',
        'Avaliação do Coordenador do Programa em Jardim:': 'avaliacao_coordenador',
        'Avaliação da assistente pedagógica do programa em Jardim:': 'avaliacao_assistente',
        'Avaliação das aulas do programa em Jardim:': 'avaliacao_aulas',
        'O que o Curso de Assistente Administrativo contribuiu para a minha vida e formação?': 'contribuicao',
        'Que outro Curso eu gostaria de fazer pelo Programa Mulheres Mil?': 'proximo_curso'
    }
    
    # Aplica o mapeamento apenas para colunas que existem no DataFrame
    existing_columns = {}
    for old_name, new_name in column_mapping.items():
        if old_name in df_clean.columns:
            existing_columns[old_name] = new_name
    
    df_clean.rename(columns=existing_columns, inplace=True)
    logs.append("Colunas renomeadas para português simplificado")
    
    # 2. Limpeza do curso (remove emoji) - se a coluna existe
    if 'curso' in df_clean.columns:
        df_clean['curso'] = df_clean['curso'].str.replace('💻', '').str.strip()
        logs.append("Emoji removido do nome do curso")
    
    # 3. Correção da FAIXA ETÁRIA (crítica!) - se a coluna existe
    if 'faixa_etaria' in df_clean.columns:
        faixas_validas = ['16 a 26 anos', '27 a 36 anos', '37 a 46 anos', '47 a 56 anos', 'Mais de 56 anos']
        
        def corrigir_faixa_etaria(valor):
            if pd.isna(valor) or valor == '':
                return 'Nao informado'
            valor = str(valor).strip()
            # Corrige o erro específico que encontramos
            if 'Ensino fundamental completo' in valor:
                return 'Nao informado'
            if valor not in faixas_validas:
                return 'Outra'
            return valor
        
        df_clean['faixa_etaria'] = df_clean['faixa_etaria'].apply(corrigir_faixa_etaria)
        logs.append("Faixa etária padronizada e corrigida")
    
    # 4. Categorização da RENDA (outro ponto crítico) - se a coluna existe
    if 'renda_original' in df_clean.columns:
        def categorizar_renda(valor):
            if pd.isna(valor) or valor == '':
                return 'Nao informado'
                
            valor = str(valor).lower().strip()
            
            if 'mais de 3' in valor:
                return 'Mais de 3 SM'
            elif '2' in valor:
                return '1-2 SM'
            elif '200' in valor:  # Captura "200,00"
                return 'Ate 1 SM'
            elif '1' in valor or 'um' in valor:
                return 'Ate 1 SM'
            elif 'menos' in valor:
                return 'Menos de 1 SM'
            else:
                return 'Nao informado'
        
        df_clean['renda_categoria'] = df_clean['renda_original'].apply(categorizar_renda)
        logs.append("Renda categorizada em faixas consistentes")
    
    # 5. Padronização de ESCOLARIDADE - se a coluna existe
    if 'escolaridade' in df_clean.columns:
        escolaridade_map = {
            'Ensino Fundamental: Anos Finais (6º ao 9º ano)': 'Ensino Fundamental',
            'Ensino Médio Completo': 'Ensino Medio',
            'Ensino Médio Incompleto': 'Ensino Medio',
            'Ensino Superior Completo': 'Ensino Superior',
            'Ensino Superior Incompleto': 'Ensino Superior'
        }
        
        def mapear_escolaridade(valor):
            valor = str(valor).strip()
            return escolaridade_map.get(valor, 'Outra')
        
        df_clean['escolaridade_simplificada'] = df_clean['escolaridade'].apply(mapear_escolaridade)
        logs.append("Escolaridade simplificada")
    
    # 6. Avaliações numéricas - para cada coluna de avaliação que existir
    avaliacao_map = {'Excelente': 5, 'Bom': 4, 'Regular': 3, 'Ruim': 2, 'Péssimo': 1}
    
    for col in ['avaliacao_coordenador', 'avaliacao_assistente', 'avaliacao_aulas']:
        if col in df_clean.columns:
            df_clean[f'{col}_score'] = df_clean[col].map(avaliacao_map).fillna(0)
    
    logs.append("Avaliações convertidas para escala numérica")
    
    return df_clean

def generate_basic_stats(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Gera estatísticas básicas do DataFrame
    """
    stats = {}
    
    # Estatísticas por coluna
    for column in df.columns:
        col_stats = {
            'data_type': str(df[column].dtype),
            'non_null_count': int(df[column].count()),
            'null_count': int(df[column].isnull().sum()),
            'unique_values': int(df[column].nunique())
        }
        
        # Para colunas numéricas
        if pd.api.types.is_numeric_dtype(df[column]):
            col_stats.update({
                'min': float(df[column].min()),
                'max': float(df[column].max()),
                'mean': float(df[column].mean()),
                'median': float(df[column].median())
            })
        
        # Para colunas de texto/categóricas
        if pd.api.types.is_string_dtype(df[column]) or pd.api.types.is_object_dtype(df[column]):
            top_values = df[column].value_counts().head(5).to_dict()
            col_stats['top_values'] = {str(k): int(v) for k, v in top_values.items()}
        
        stats[column] = col_stats
    
    return stats

def get_column_types(df: pd.DataFrame) -> Dict[str, str]:
    """
    Retorna os tipos de dados de cada coluna
    """
    return {col: str(dtype) for col, dtype in df.dtypes.items()}