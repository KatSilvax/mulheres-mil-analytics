# backend/app/services/analysis_service.py
import pandas as pd
import re
from typing import Dict, Any, List
from collections import Counter

# --- Funções de Análise Específicas ---

def analisar_desafios(desafios_series: pd.Series) -> Dict[str, int]:
    """Analisa e categoriza os desafios mais mencionados."""
    texto = ' '.join(desafios_series.dropna().astype(str)).lower()
    palavras_chave = {
        'transporte': ['transporte', 'ônibus', 'onibus', 'locomoção', 'deslocamento', 'distancia', 'distância'],
        'tempo': ['tempo', 'horário', 'cronograma', 'corrido'],
        'material': ['material', 'camiseta', 'uniforme', 'kit', 'apostila'],
        'financeiro': ['dinheiro', 'bolsa', 'recurso', 'financeiro', 'passagem'],
        'familia': ['filho', 'filhos', 'criança', 'crianças', 'família', 'casa', 'marido'],
        'saude': ['saúde', 'doença', 'medicamento', 'cansada']
    }
    
    resultados = {}
    for categoria, palavras in palavras_chave.items():
        count = sum(texto.count(palavra) for palavra in palavras)
        if count > 0:
            resultados[categoria] = count
    
    return dict(sorted(resultados.items(), key=lambda item: item[1], reverse=True))

def extrair_insights_desafios(analise_desafios: Dict[str, int]) -> List[str]:
    """Extrai insights acionáveis dos desafios."""
    insights = []
    if 'transporte' in analise_desafios:
        insights.append("💡 A dificuldade com transporte e distância é um obstáculo significativo. Avaliar a possibilidade de auxílio-transporte.")
    if 'familia' in analise_desafios:
        insights.append("💡 A conciliação com as responsabilidades familiares é um desafio. Considerar horários flexíveis ou atividades para os filhos.")
    if 'tempo' in analise_desafios:
        insights.append("💡 A gestão do tempo é uma barreira. Oficinas sobre organização podem ser um complemento valioso ao curso.")
    return insights

def analisar_proximos_cursos(cursos_series: pd.Series) -> Dict[str, int]:
    """Analisa e categoriza os cursos mais solicitados."""
    cursos = cursos_series.dropna().astype(str)
    cursos = cursos[cursos != '']
    
    cursos_limpos = []
    for curso in cursos:
        curso = curso.strip().lower()
        if 'informática' in curso or 'computação' in curso or 'computador' in curso:
            cursos_limpos.append('Informática')
        elif 'enfermagem' in curso or 'cuidador' in curso or 'saude' in curso:
            cursos_limpos.append('Área da Saúde/Cuidado')
        elif 'costura' in curso:
            cursos_limpos.append('Corte e Costura')
        elif 'estética' in curso or 'beleza' in curso or 'cabelo' in curso:
            cursos_limpos.append('Beleza e Estética')
        elif 'eletricista' in curso:
            cursos_limpos.append('Elétrica')
        else:
            cursos_limpos.append(curso.capitalize())
            
    return dict(Counter(cursos_limpos).most_common(10))

def gerar_recomendacoes(analysis: Dict[str, Any]) -> List[str]:
    """Gera recomendações acionáveis baseadas na análise completa."""
    recomendacoes = []
    
    # Recomendações baseadas em desafios
    desafios = analysis.get('desafios', {}).get('principais_desafios', {})
    if 'transporte' in desafios:
        recomendacoes.append("AVALIAR a implementação de um auxílio-transporte, especialmente para alunas de baixa renda, ou parcerias com o transporte municipal.")
    
    # Recomendações baseadas em demanda futura
    demanda = analysis.get('demanda_futura', {}).get('cursos_mais_solicitados', {})
    if demanda:
        cursos_mais_procurados = list(demanda.keys())[:3]
        recomendacoes.append(f"PRIORIZAR a oferta de novos cursos nas áreas de: {', '.join(cursos_mais_procurados)}, que demonstraram alta demanda.")
        
    # Recomendações baseadas em satisfação
    media_aulas = analysis.get('satisfacao', {}).get('medias_avaliacoes', {}).get('aulas', 0)
    if media_aulas > 4.5:
         recomendacoes.append(f"MANTER a alta qualidade do corpo docente, que obteve uma excelente avaliação média de {media_aulas:.2f} de 5.")

    return recomendacoes

def identificar_insight_faixa_etaria(df: pd.DataFrame) -> str:
    """Identifica insights sobre a distribuição etária."""
    distribuicao = df['faixa_etaria'].value_counts()
    if distribuicao.get('37 a 46 anos', 0) > distribuicao.get('16 a 26 anos', 0):
        return "A maior parte das participantes são mulheres maduras (37-46 anos), indicando que o programa atrai um público que busca reinserção ou aprimoramento profissional."
    return "O perfil etário das participantes é diversificado, abrangendo diferentes fases da vida."

# --- Função Principal de Geração da Análise ---

def generate_detailed_analysis(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Gera análise detalhada dos dados padronizados para políticas públicas,
    com todos os tipos de dados compatíveis com JSON.
    """
    analysis = {}
    
    # Função auxiliar para converter dicionários de value_counts para JSON-safe
    def to_safe_dict(series: pd.Series) -> Dict[str, int]:
        return {str(k): int(v) for k, v in series.to_dict().items()}

    # 1. ANÁLISE DEMOGRÁFICA (Perfil das mulheres)
    analysis['perfil_demografico'] = {
        'distribuicao_idade': to_safe_dict(df['faixa_etaria'].value_counts()),
        'distribuicao_escolaridade': to_safe_dict(df['escolaridade_simplificada'].value_counts()),
        'distribuicao_renda': to_safe_dict(df['renda_categoria'].value_counts()),
        'total_mulheres': int(len(df)),
        'insight_idade': identificar_insight_faixa_etaria(df)
    }
    
    # 2. ANÁLISE DE SATISFAÇÃO (Qualidade do programa)
    analysis['satisfacao'] = {
        'medias_avaliacoes': {
            'coordenador': float(round(df['avaliacao_coordenador_score'].mean(), 2)),
            'assistente_pedagogica': float(round(df['avaliacao_assistente_score'].mean(), 2)),
            'aulas': float(round(df['avaliacao_aulas_score'].mean(), 2)),
            'geral': float(round(df[['avaliacao_coordenador_score', 'avaliacao_assistente_score', 'avaliacao_aulas_score']].mean().mean(), 2))
        },
        'distribuicao_avaliacoes': {
            'coordenador': to_safe_dict(df['avaliacao_coordenador'].value_counts()),
            'assistente_pedagogica': to_safe_dict(df['avaliacao_assistente'].value_counts()),
            'aulas': to_safe_dict(df['avaliacao_aulas'].value_counts())
        }
    }
    
    # 3. ANÁLISE DE DESAFIOS (Para melhorar o programa)
    desafios_analisados = analisar_desafios(df['desafios'])
    analysis['desafios'] = {
        'principais_desafios': desafios_analisados,
        'insights_acionaveis': extrair_insights_desafios(desafios_analisados)
    }
    
    # 4. DEMANDA FUTURA (Cursos desejados)
    analysis['demanda_futura'] = {
        'cursos_mais_solicitados': analisar_proximos_cursos(df['proximo_curso'])
    }
    
    # 5. METRICS DE QUALIDADE DOS DADOS
    analysis['qualidade_dados'] = {
        'taxa_resposta': 100.0,
        'dados_faltantes': {
            'faixa_etaria': int((df['faixa_etaria'] == 'Nao informado').sum()),
            'renda': int((df['renda_categoria'] == 'Nao informado').sum()),
            'profissao': int((df['profissao'] == '').sum())
        }
    }
    
    # 6. RECOMENDAÇÕES PARA POLÍTICAS PÚBLICAS
    analysis['recomendacoes'] = gerar_recomendacoes(analysis)
    
    return analysis
