from prefect import task, get_run_logger
import pandas as pd
from pathlib import Path

# Raiz do projeto: .../prefect_flows/pipeline_dre_lancamentos
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Camadas
DATA_STAGING_DIR = PROJECT_ROOT / "data" / "staging"     # entra parquet "bruto"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"  # sai parquet concatenado


@task
def concatenar_parquet():
    logger = get_run_logger()

    logger.info(f"Pasta de entrada (STAGING):  {DATA_STAGING_DIR}")
    logger.info(f"Pasta de saída (PROCESSED): {DATA_PROCESSED_DIR}")

    if not DATA_STAGING_DIR.exists():
        raise FileNotFoundError(f"Pasta de staging não existe: {DATA_STAGING_DIR}")

    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # Arquivos parquet na staging, exceto o consolidado
    arquivos = [
        p for p in DATA_STAGING_DIR.glob("*.parquet")
        if p.name != "dados_concatenados.parquet"
    ]

    if not arquivos:
        logger.warning("Nenhum arquivo Parquet encontrado para concatenar.")
        return

    dfs = []
    for caminho in arquivos:
        logger.info(f"Lendo {caminho.name}...")
        try:
            df = pd.read_parquet(caminho, engine="pyarrow")
            dfs.append(df)
        except Exception as e:
            logger.error(f"Erro ao ler {caminho.name}: {e}")
            # Se quiser seguir mesmo com erro em um arquivo, remova o raise
            raise

    if not dfs:
        logger.error("Nenhum DataFrame válido para concatenar.")
        raise ValueError("Nenhum DataFrame válido para concatenar.")

    # 1) Concatena
    df = pd.concat(dfs, ignore_index=True)

    # 2) Deixa o pandas sugerir dtypes estáveis
    df = df.convert_dtypes()

    # 3) Tenta converter colunas texto→número
    for c in df.columns:
        if pd.api.types.is_object_dtype(df[c]) or pd.api.types.is_string_dtype(df[c]):
            df[c] = pd.to_numeric(df[c], errors="ignore")

    # 4) Tratar texto: vira string e limpa sufixo ".0"
    texto_cols = df.select_dtypes(include=["object", "string"]).columns
    for c in texto_cols:
        df[c] = df[c].astype("string").fillna("")
        df[c] = df[c].str.replace(r"\.0$", "", regex=True)

    # 5) Salva na camada PROCESSED
    caminho_final = DATA_PROCESSED_DIR / "dados_concatenados.parquet"
    df.to_parquet(caminho_final, engine="pyarrow", index=False)

    logger.info(f"Arquivo concatenado salvo em: {caminho_final}")
