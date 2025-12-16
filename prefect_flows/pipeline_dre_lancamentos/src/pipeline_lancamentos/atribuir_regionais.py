from pathlib import Path
import pandas as pd
import psycopg2
from prefect import task, get_run_logger

from utils import get_db_config

# Base do projeto: .../prefect_flows/pipeline_dre_lancamentos
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
PROCESSED_DIR = DATA_DIR / "processed"


@task
def atribuir_regionais() -> None:
    logger = get_run_logger()

    caminho_entrada = PROCESSED_DIR / "dados_limpos.parquet"
    caminho_saida = PROCESSED_DIR / "dados_com_regionais.parquet"

    logger.info(f"Arquivo de entrada (dados limpos): {caminho_entrada}")
    logger.info(f"Arquivo de saída (com regionais): {caminho_saida}")

    if not caminho_entrada.exists():
        raise FileNotFoundError(
            f"Arquivo de dados limpos não encontrado: {caminho_entrada}"
        )

    # 1) Carrega os dados limpos (já com id_cidade)
    df = pd.read_parquet(caminho_entrada)
    logger.info(f"Total de linhas em dados_limpos: {len(df)}")

    if "id_cidade" not in df.columns:
        raise ValueError(
            "Coluna 'id_cidade' não encontrada em dados_limpos. "
            "Certifique-se de que a etapa de limpeza está atribuindo o id_cidade."
        )

    # 2) Buscar mapeamento cidade x regional x regcid no DW
    logger.info("Conectando ao DW para carregar dim_regional_cidade...")
    conn = psycopg2.connect(**get_db_config("dw"))
    try:
        dim_regcid = pd.read_sql_query(
            """
            SELECT
                id_cidade,
                id_regcid,
                id_regional
            FROM dim_regional_cidade;
            """,
            conn,
        )
    finally:
        conn.close()

    logger.info(
        f"dim_regional_cidade carregada com {len(dim_regcid)} linhas e "
        f"{dim_regcid['id_cidade'].nunique()} cidades distintas."
    )

    # 3) Join por id_cidade (cada cidade em uma única regional)
    df = df.merge(
        dim_regcid,
        on="id_cidade",
        how="left",
        validate="m:1",  # muitos lançamentos para 1 cidade na dimensão
    )

    # 4) Tipos amigáveis (mantendo possibilidade de nulos)
    for col in ["id_regcid", "id_regional"]:
        if col in df.columns:
            df[col] = df[col].astype("Int64")  # inteiro com suporte a <NA>

    # 5) Log de cidades sem regional (se houver inconsistência no DW)
    faltando = df[df["id_regcid"].isna()]["id_cidade"].unique()
    if len(faltando) > 0:
        logger.warning(
            f"{len(faltando)} id_cidade não encontradas na dim_regional_cidade: "
            f"{faltando}"
        )
    else:
        logger.info("Todas as cidades presentes em dados_limpos possuem regional atribuída.")

    # 6) Salvar resultado
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(caminho_saida, index=False)
    logger.info(f"Arquivo com regionais salvo em: {caminho_saida}")
