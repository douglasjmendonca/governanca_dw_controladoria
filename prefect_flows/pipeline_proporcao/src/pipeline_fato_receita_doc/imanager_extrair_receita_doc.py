from prefect import task
from prefect.logging import get_run_logger

import pandas as pd
import psycopg2
from pathlib import Path
from datetime import datetime, date
from dotenv import load_dotenv

from utils import get_db_config


def _project_root() -> Path:
    # arquivo em: <raiz>/src/pipeline_receita_doc/imanager_extrair_receita_doc.py
    return Path(__file__).resolve().parents[2]


def _janela_mensal(data_ref: str) -> tuple[date, date]:
    """
    Recebe YYYY-MM-DD e devolve (start_date, end_date) como 1º dia do mês e 1º dia do mês seguinte.
    """
    ref = datetime.strptime(data_ref, "%Y-%m-%d").date()
    start_date = ref.replace(day=1)
    if start_date.month == 12:
        end_date = date(start_date.year + 1, 1, 1)
    else:
        end_date = date(start_date.year, start_date.month + 1, 1)
    return start_date, end_date


SQL_RECEITA_DOC_MENSAL = """
SELECT
  a."Regional",
  a."id_regional_imanager",
  a."Cidade",
  a."id_cidade_imanager",
  date_trunc('month', a."Data_Faturamento")::date AS "Faturamento",
  a."T_doc",
  COUNT(DISTINCT a."Id_Doc")  AS qtd_docs,
  SUM(a.valor_lancamento)     AS valor_total
FROM (
  SELECT
    r.descricao          AS "Regional",
    r.id                 AS "id_regional_imanager",
    cid.nomedacidade     AS "Cidade",
    cid.id               AS "id_cidade_imanager",
    dr.d_datafaturamento AS "Data_Faturamento",
    dr.valordocumento    AS valor_lancamento,
    CASE
      WHEN length(regexp_replace(COALESCE(cli.cpf_cnpj, ''), '[^0-9]', '', 'g')) <= 11
        THEN 'CPF' ELSE 'CNPJ'
    END                  AS "T_doc",
    dr.id                AS "Id_Doc"
  FROM public.docreceber dr
  JOIN public.clientes cli
    ON cli.cidade = dr.codigodacidade
   AND cli.codigocliente = dr.cliente
  JOIN public.cidade cid
    ON cid.codigodacidade = dr.codigodacidade
  JOIN public.regional r
    ON r.codigo = cid.codigo_regional
  WHERE
    dr.d_datafaturamento >= %s
    AND dr.d_datafaturamento <  %s
    AND (dr.boletoequipamento IS NULL OR dr.boletoequipamento = 0)
    AND (dr.reparcelamento    IS NULL OR dr.reparcelamento = 0)
) a
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1, 3, 5, 6;
"""


@task(name="extrair_receita_doc_imanager")
def extrair_receita_doc_imanager(data_ref: str, db: str = "imanager") -> str:
    """
    Extrai agregação mensal por CPF/CNPJ (qtd_docs, valor_total) e salva em data/raw.
    Saída: data/raw/receita_doc_imanager_YYYY-MM-01.parquet
    """
    logger = get_run_logger()
    start_date, end_date = _janela_mensal(data_ref)

    root = _project_root()
    raw_dir = root / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    # padrão de nome (mantém coerência e facilita rodar mensal)
    out_file = raw_dir / f"receita_doc_imanager_{start_date.isoformat()}.parquet"

    logger.info(f"Iniciando extração receita_doc | data_ref={data_ref}")
    logger.info(f"Janela mensal: {start_date} -> {end_date}")
    logger.info(f"Arquivo de saída: {out_file}")

    load_dotenv()
    cfg = get_db_config(db)

    logger.info(f"Conectando: host={cfg.get('host')} database={cfg.get('database')} user={cfg.get('user')}")
    with psycopg2.connect(**cfg) as conn:
        df = pd.read_sql_query(SQL_RECEITA_DOC_MENSAL, conn, params=(start_date, end_date))

    logger.info(f"Registros extraídos: {len(df):,}")
    df.to_parquet(out_file, index=False)

    logger.info("Extração finalizada com sucesso.")
    return str(out_file)
