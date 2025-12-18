from prefect import task
from prefect.logging import get_run_logger

import pandas as pd
import psycopg2
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

from utils import get_db_config


SQL_ULTIMA_ORDEM = """
WITH UltimaOrdem AS (
  SELECT
    pc.dataposicao,
    r.descricao              AS regional,
    r.id                     As id_regional_imanager,
    cid.nomedacidade         AS cidade,
    cid.id                   As id_cidade_imanager,
    ct.id                    AS id_contrato,
    CASE
      WHEN length(regexp_replace(COALESCE(cli.cpf_cnpj, ''), '[^0-9]', '', 'g')) <= 11 THEN 'CPF'
      ELSE 'CNPJ'
    END                      AS tipo_documento,
    pc.descricaosituacaocontrato,
    SUM(pc.valorpacotedesconto) AS valor
  FROM gerencial.pacotesdiarios pc
  LEFT JOIN contratos ct
    ON ct.id = pc.idcontrato
  LEFT JOIN cidade cid
    ON cid.codigodacidade = ct.cidade
  LEFT JOIN regional r
    ON r.codigo = cid.codigo_regional
  LEFT JOIN clientes cli
    ON cli.cidade = ct.cidade
   AND cli.codigocliente = ct.codigodocliente
  WHERE pc.dataposicao = %s::date
    AND COALESCE(cli.nome, '')        NOT ILIKE '%%MASTER%%'
    AND COALESCE(cid.nomedacidade,'') NOT ILIKE '%%TESTE%%'
    AND COALESCE(cli.nome, '')        NOT ILIKE '%%RBC%%'
  GROUP BY 1,2,3,4,5,6,7,8
)
SELECT
  dataposicao AS data_posicao,
  id_regional_imanager,
  regional,
  id_cidade_imanager,
  cidade,
  descricaosituacaocontrato,
  tipo_documento,
  COUNT(DISTINCT id_contrato) AS contratos,
  SUM(valor) AS valor
FROM UltimaOrdem
GROUP BY 1,2,3,4,5,6,7;
"""


# ✅ salva na pasta data/ (na raiz do projeto), mesmo executando dentro de src/
# arquivo está em: <raiz>/src/pipeline_clientes/imanager_extrair_parquet.py
# raiz do projeto: parents[2]  -> <raiz>
PROJECT_ROOT = Path(__file__).resolve().parents[2]
PASTA_SAIDA = PROJECT_ROOT / "data" / "raw"


@task(name="extrair_clientes_imanager")
def imanager_extrair_para_parquet(
    data_ref: str,
    db: str = "imanager",
) -> str:
    """
    Executa a consulta e salva o resultado em data/raw/clientes_imanager_<data_ref>.parquet
    (sem alterar o utils.py).
    """
    logger = get_run_logger()

    # valida formato da data
    datetime.strptime(data_ref, "%Y-%m-%d")

    PASTA_SAIDA.mkdir(parents=True, exist_ok=True)
    out_file = PASTA_SAIDA / f"clientes_imanager_{data_ref}.parquet"

    logger.info(f"Iniciando extração de clientes | data_ref={data_ref}")
    logger.info(f"Arquivo de saída: {out_file}")

    load_dotenv()

    cfg = get_db_config(db)
    # log leve (sem senha)
    logger.info(
        f"Conectando: host={cfg.get('host')} database={cfg.get('database')} user={cfg.get('user')}"
    )

    with psycopg2.connect(**cfg) as conn:
        df = pd.read_sql_query(SQL_ULTIMA_ORDEM, conn, params=(data_ref,))

    logger.info(f"Registros extraídos: {len(df):,}")

    df.to_parquet(out_file, index=False)

    logger.info("Extração finalizada com sucesso.")
    return str(out_file)
