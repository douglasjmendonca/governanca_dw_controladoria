from prefect import task, get_run_logger
import pandas as pd
from pathlib import Path

# Raiz do projeto dentro do container (ex.: /app)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DATA_STAGING_DIR = PROJECT_ROOT / "data" / "staging"


@task
def converter_parquet():
    logger = get_run_logger()

    logger.info(f"Pasta de entrada (RAW): {DATA_RAW_DIR}")
    logger.info(f"Pasta de saída (STAGING): {DATA_STAGING_DIR}")

    DATA_STAGING_DIR.mkdir(parents=True, exist_ok=True)

    if not DATA_RAW_DIR.exists():
        raise FileNotFoundError(f"Pasta de entrada não existe: {DATA_RAW_DIR}")

    arquivos = [
        f for f in DATA_RAW_DIR.iterdir()
        if f.suffix.lower() in [".xlsx", ".xls"]
    ]

    if not arquivos:
        logger.warning(f"Nenhum arquivo Excel encontrado em {DATA_RAW_DIR}")
        return

    for caminho in arquivos:
        logger.info(f"Lendo {caminho.name}...")

        try:
            df = pd.read_excel(caminho)
            nome_base = caminho.stem
            caminho_parquet = DATA_STAGING_DIR / f"{nome_base}.parquet"

            df.to_parquet(caminho_parquet, index=False)
            logger.info(f"Salvo como Parquet em: {caminho_parquet}")
        except Exception as e:
            logger.error(f"Erro ao processar {caminho.name}: {e}")
