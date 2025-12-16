from pathlib import Path
from prefect import task, get_run_logger
import pandas as pd


# Base do projeto: .../prefect_flows/pipeline_dre_lancamentos
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
PROCESSED_DIR = DATA_DIR / "processed"
MODELO_DIR = DATA_DIR / "modelo"


@task
def classificar_dre() -> None:
    """
    Lê:
      - data/processed/dados_com_regionais.parquet
      - data/modelo/plano_contas_pco_novo.xlsx

    E devolve:
      - data/processed/dre_classificado.parquet

    Adicionando as colunas-dimensão:
      - conta   (texto da conta)
      - item    (ITEM DA DRE)
      - natureza
      - detalhe (DETALHES DO ITEM DA DRE)

    E calculando VALORG a partir de TOTAL RE,
    respeitando o sinal econômico que já vem no arquivo de origem.
    """
    logger = get_run_logger()

    caminho_dre = PROCESSED_DIR / "dados_com_regionais.parquet"
    caminho_pco = MODELO_DIR / "plano_contas_pco_novo.xlsx"
    caminho_saida = PROCESSED_DIR / "dre_classificado.parquet"

    logger.info(f"Entrada DRE: {caminho_dre}")
    logger.info(f"Plano de contas: {caminho_pco}")
    logger.info(f"Saída classificada: {caminho_saida}")

    # Verificações básicas
    if not caminho_dre.exists():
        raise FileNotFoundError(f"Arquivo de DRE não encontrado: {caminho_dre}")

    if not caminho_pco.exists():
        raise FileNotFoundError(f"Plano de contas não encontrado: {caminho_pco}")

    # 1) Carregar dados
    dre = pd.read_parquet(caminho_dre)
    pcontas = pd.read_excel(caminho_pco)

    # 2) Padronizar textos (CONTA, ITEM, DETALHES, NATUREZA)
    def normaliza_str(s):
        if pd.isna(s):
            return ""
        # tira espaços duplicados e espaços nas pontas
        return " ".join(str(s).split())

    # Normaliza CONTA no DRE
    if "CONTA" in dre.columns:
        dre["CONTA"] = dre["CONTA"].astype(str).map(normaliza_str)
    else:
        raise ValueError("Coluna 'CONTA' não encontrada no DRE.")

    # Normaliza colunas do plano de contas
    for col in ["CONTA", "ITEM DA DRE", "DETALHES DO ITEM DA DRE", "NATUREZA"]:
        if col not in pcontas.columns:
            raise ValueError(f"Coluna '{col}' não encontrada no plano de contas.")
        pcontas[col] = pcontas[col].map(normaliza_str)

    # 3) Dicionários de mapeamento
    dic_item = pcontas.set_index("CONTA")["ITEM DA DRE"].to_dict()
    dic_detalhe = pcontas.set_index("CONTA")["DETALHES DO ITEM DA DRE"].to_dict()
    dic_natureza = pcontas.set_index("CONTA")["NATUREZA"].to_dict()

    # 4) Aplicar mapeamentos – criando as colunas-dimensão
    dre["conta"] = dre["CONTA"]                     # natural key textual
    dre["item"] = dre["CONTA"].map(dic_item)
    dre["detalhe"] = dre["CONTA"].map(dic_detalhe)
    dre["natureza"] = dre["CONTA"].map(dic_natureza)

    # Log de contas sem classificação
    faltando_item = dre[dre["item"].isna()]["CONTA"].unique()
    if len(faltando_item) > 0:
        logger.warning(
            f"{len(faltando_item)} contas não encontradas no plano de contas. "
            f"Exemplo(s): {faltando_item[:10]}"
        )
    else:
        logger.info("Todas as contas foram classificadas no plano de contas.")

    # 5) Cálculo do VALORG (usando TOTAL RE diretamente)
    if "TOTAL RE" not in dre.columns:
        raise ValueError(
            "Coluna 'TOTAL RE' não encontrada no DRE para cálculo de VALORG."
        )

    # Mantém o sinal econômico que já vem do arquivo de origem
    dre["VALORG"] = dre["TOTAL RE"]

    # 6) Salvar resultado
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    dre.to_parquet(caminho_saida, index=False)
    logger.info(f"DRE classificado salvo em: {caminho_saida}")
