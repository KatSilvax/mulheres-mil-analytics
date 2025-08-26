
import pandas as pd
import io
import logging
from typing import Dict, Any, Tuple

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
    
    # 4. Log de colunas identificadas
    logs.append(f"Colunas identificadas: {', '.join(df_clean.columns)}")
    
    return df_clean, logs

def generate_basic_stats(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Gera estatísticas básicas do DataFrame
    """
    stats = {}
    
    # Estatísticas por coluna
    for column in df.columns:
        col_stats = {
            'data_type': str(df[column].dtype),
            'non_null_count': int[column].count(),
            'null_count': int[column].isnull().sum(),
            'unique_values': int[column].nunique()
        }
        
        # Para colunas numéricas
        if pd.api.types.is_numeric_dtype(df[column]):

            #Verificação para evitar erros em colunas vazias
            if not df[column].dropna().empty:
                col_stats.update({
                'min': float(df[column].min()),
                'max': float(df[column].max()),
                'mean': float(df[column].mean()),
                'median': float(df[column].median())
            })
        
        # Para colunas de texto/categóricas
        if pd.api.types.is_string_dtype(df[column]) or pd.api.types.is_object_dtype(df[column]):
            top_values_series = df[column].value_counts().head(5)
            
            top_values = {str(key): int(value) for key, value in top_values_series.items()}
            col_stats['top_values'] = top_values
        
        stats[column] = col_stats
    
    return stats

def get_column_types(df: pd.DataFrame) -> Dict[str, str]:
    """
    Retorna os tipos de dados de cada coluna
    """
    return {col: str(dtype) for col, dtype in df.dtypes.items()}