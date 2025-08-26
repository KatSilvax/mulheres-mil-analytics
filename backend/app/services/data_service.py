
import pandas as pd
import io

def process_csv_data(file):
    """Processa o arquivo CSV enviado"""
    try:
        # Lê o arquivo CSV
        df = pd.read_csv(io.StringIO(file.stream.read().decode('UTF-8')))
        
        # Retorna informações básicas para teste
        return {
            'message': 'Arquivo processado com sucesso',
            'row_count': len(df),
            'columns': list(df.columns),
            'preview': df.head().to_dict(orient='records')
        }
        
    except Exception as e:
        raise Exception(f'Erro ao processar CSV: {str(e)}')