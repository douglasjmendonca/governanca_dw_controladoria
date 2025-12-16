from prefect import task, get_run_logger
import pandas as pd
from pathlib import Path
import unicodedata
import psycopg2
import sys

# === Descobrir pastas a partir deste arquivo ===
FILE_PATH = Path(__file__).resolve()
SRC_DIR = FILE_PATH.parents[1]        # .../prefect_flows/pipeline_dre_lancamentos/src
PROJECT_ROOT = FILE_PATH.parents[2]   # .../prefect_flows/pipeline_dre_lancamentos

# Garantir que o src está no sys.path para conseguir importar utils.py
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from utils import get_db_config  # agora deve funcionar

# === Paths base (funciona local e em Docker) ===
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DATA_STAGING_DIR = PROJECT_ROOT / "data" / "staging"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


# === Mapeamento de valores "estranhos" de cidade para a forma certa ===
MAPEAMENTO_CIDADES = {
    "BETIM REGIONAL": "BETIM",
    "DIVINOPOLIS REGIONAL": "DIVINOPOLIS",
    "ITAJUBA REGIONAL": "ITAJUBA",
    "ITAUNA REGIONAL": "ITAUNA",
    "LAVRAS REGIONAL": "LAVRAS",
    "PASSOS REGIONAL": "PASSOS",
    "SAO PAULO": "TAUBATE",
    "SAO PAULO REGIONAL": "TAUBATE",
    "SAPUCAI MIRIM": "SAPUCAI-MIRIM",
    "UNAI REGIONAL": "UNAI",
    "VARGINHA REGIONAL": "VARGINHA",
}


def remover_acentos(texto: str) -> str:
    """Remove acentos de uma string (NFKD -> ASCII)."""
    if not isinstance(texto, str):
        if pd.isna(texto):
            return ""
        texto = str(texto)
    nfkd = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


@task
def limpar_dados():
    logger = get_run_logger()

    # Agora usamos data/processed tanto para entrada quanto para saída
    caminho_entrada = DATA_PROCESSED_DIR / "dados_concatenados.parquet"
    caminho_saida = DATA_PROCESSED_DIR / "dados_limpos.parquet"

    logger.info(f"Arquivo de entrada: {caminho_entrada}")
    logger.info(f"Arquivo de saída:  {caminho_saida}")

    if not caminho_entrada.exists():
        logger.error(
            "Arquivo concatenado não encontrado. "
            "Certifique-se de rodar a task 'concatenar_parquet' antes."
        )
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho_entrada}")

    logger.info("Carregando dados para limpeza...")
    df = pd.read_parquet(caminho_entrada)

    logger.info(f"Total de linhas antes da limpeza: {len(df)}")

    # ==== Validação básica de colunas esperadas ====
    colunas_necessarias = ["CONTA", "CIDADE APÓS RATEIO", "DATA"]
    faltando = [c for c in colunas_necessarias if c not in df.columns]
    if faltando:
        msg = f"Colunas obrigatórias ausentes no DataFrame: {faltando}"
        logger.error(msg)
        raise ValueError(msg)

    # ==== Limpeza de CONTA ====
    df["CONTA"] = df["CONTA"].astype(str)
    df["CONTA"] = df["CONTA"].apply(lambda x: " ".join(x.split()))

    # ==== Normalização de CIDADE APÓS RATEIO ====
    df["CIDADE APÓS RATEIO"] = df["CIDADE APÓS RATEIO"].astype(str)
    df["CIDADE APÓS RATEIO"] = df["CIDADE APÓS RATEIO"].apply(
        lambda x: " ".join(x.split())
    )
    df["CIDADE APÓS RATEIO"] = df["CIDADE APÓS RATEIO"].str.upper()
    df["CIDADE APÓS RATEIO"] = df["CIDADE APÓS RATEIO"].apply(remover_acentos)
    df["CIDADE APÓS RATEIO"] = df["CIDADE APÓS RATEIO"].str.strip()

    # Aplica mapeamento (REGIONAL, grafias diferentes, etc.)
    df["CIDADE APÓS RATEIO"] = df["CIDADE APÓS RATEIO"].replace(MAPEAMENTO_CIDADES)

    # ==== Buscar dimensão de cidades no DW ====
    logger.info("Buscando dimensão de cidades no DW (schema controladoria)...")
    try:
        conn = psycopg2.connect(**get_db_config("dw"))
        # Ajuste aqui o nome da tabela/colunas se no DW estiver diferente
        dim_cidades = pd.read_sql_query(
            "SELECT id_cidade, nome_cidade FROM dim_cidades;",
            conn,
        )
    finally:
        conn.close()

    # Normaliza nome_cidade da dimensão para bater com CIDADE APÓS RATEIO
    dim_cidades["nome_cidade"] = dim_cidades["nome_cidade"].astype(str)
    dim_cidades["nome_cidade_norm"] = (
        dim_cidades["nome_cidade"]
        .str.upper()
        .map(remover_acentos)
        .str.strip()
    )

    # ==== Join para trazer id_cidade ====
    logger.info("Realizando join com dim_cidades para obter id_cidade...")
    df = df.merge(
        dim_cidades[["id_cidade", "nome_cidade_norm"]],
        left_on="CIDADE APÓS RATEIO",
        right_on="nome_cidade_norm",
        how="left",
    )

    # Checar linhas sem match
    linhas_sem_match = df["id_cidade"].isna().sum()
    if linhas_sem_match > 0:
        cidades_sem_match = (
            df.loc[df["id_cidade"].isna(), "CIDADE APÓS RATEIO"]
            .drop_duplicates()
            .tolist()
        )
        logger.warning(
            f"{linhas_sem_match} linhas ficaram sem id_cidade após o join. "
            f"Cidades sem match: {cidades_sem_match}"
        )

    # Não precisamos mais da coluna auxiliar do join
    df.drop(columns=["nome_cidade_norm"], inplace=True)

    # ==== Datas, mês, ano, nome do mês ====
    df["DATA"] = pd.to_datetime(df["DATA"], errors="coerce")

    if df["DATA"].isna().all():
        logger.warning(
            "Todas as datas ficaram NaT após conversão. "
            "Verificar formato da coluna 'DATA'."
        )

    df["MES"] = df["DATA"].dt.month
    df["ANO"] = df["DATA"].dt.year

    meses_dict = {
        1: "Janeiro",
        2: "Fevereiro",
        3: "Março",
        4: "Abril",
        5: "Maio",
        6: "Junho",
        7: "Julho",
        8: "Agosto",
        9: "Setembro",
        10: "Outubro",
        11: "Novembro",
        12: "Dezembro",
    }
    df["NMES"] = df["MES"].map(meses_dict)

    meses_encontrados = sorted(df["MES"].dropna().unique().tolist())
    logger.info(f"Meses encontrados nos dados: {meses_encontrados}")

    logger.info(f"Total de linhas após limpeza: {len(df)}")

    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(caminho_saida, index=False)

    logger.info(f"Dados limpos salvos em: {caminho_saida}")
