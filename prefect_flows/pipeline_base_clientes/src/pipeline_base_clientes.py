from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from prefect import flow
from prefect.logging import get_run_logger

from pipeline_clientes.imanager_extair_parquet import imanager_extrair_para_parquet
from pipeline_clientes.limpar_clientes import limpar_clientes
from pipeline_clientes.preparar_fato_clientes import preparar_fato_clientes
from pipeline_clientes.carregar_fato_clientes import carregar_fato_clientes


def _validate_data_ref(data_ref: str) -> str:
    """
    Valida e padroniza a data de referência no formato YYYY-MM-DD.
    """
    datetime.strptime(data_ref, "%Y-%m-%d")
    return data_ref


@flow(name="pipeline_clientes")
def pipeline_clientes(
    data_ref: Optional[str] = None,
    origem_db: str = "imanager",
    aplicar_dedup: bool = True,
) -> None:
    logger = get_run_logger()
    logger.info("Iniciando pipeline de clientes...")

    # Fallback apenas para execuções não interativas
    if not data_ref:
        data_ref = date.today().isoformat()

    data_ref = _validate_data_ref(data_ref)
    logger.info(f"Data de referência: {data_ref}")
    logger.info(f"Origem: {origem_db} | Dedup: {aplicar_dedup}")

  
    caminho_raw = imanager_extrair_para_parquet(
        data_ref=data_ref,
        db=origem_db,
    )
    logger.info(f"✔ Extração finalizada. Arquivo raw salvo em: {caminho_raw}")

   
    limpar_clientes(aplicar_dedup=aplicar_dedup)
    preparar_fato_clientes()
    carregar_fato_clientes()

    logger.info("Pipeline de clientes finalizado com sucesso.")


if __name__ == "__main__":
    # Execução manual/local (entrada interativa da data)
    data_ref = input("Digite a data de referência (YYYY-MM-DD): ").strip()
    pipeline_clientes(data_ref=data_ref)
