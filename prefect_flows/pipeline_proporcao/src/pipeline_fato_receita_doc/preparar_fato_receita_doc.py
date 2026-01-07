from prefect import task
from prefect.logging import get_run_logger

import pandas as pd
import psycopg2
from pathlib import Path
from datetime import datetime

from utils import get_db_config


def _project_root() -> Path:
    # arquivo está em: <raiz>/src/pipeline_fato_receita_doc/preparar_fato_receita_doc.py
    return Path(__file__).resolve().parents[2]


def _encontrar_ultimo_raw(raw_dir: Path) -> Path:
    arquivos = sorted(raw_dir.glob("receita_doc_imanager_*.parquet"))
    if not arquivos:
        raise FileNotFoundError(
            f"Nenhum arquivo encontrado em: {raw_dir} (padrão receita_doc_imanager_*.parquet)"
        )
    return arquivos[-1]


def _get_columns(conn, schema: str, table: str) -> list[str]:
    q = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name   = %s
        ORDER BY ordinal_position;
    """
    return pd.read_sql_query(q, conn, params=(schema, table))["column_name"].tolist()


def _detectar_coluna_existente(cols_existentes: list[str], candidatas: list[str], contexto: str) -> str:
    for c in candidatas:
        if c in cols_existentes:
            return c
    raise ValueError(
        f"Não encontrei nenhuma das colunas {candidatas} em {contexto}. "
        f"Colunas existentes: {cols_existentes}"
    )


@task(name="preparar_fato_receita_doc")
def preparar_fato_receita_doc(schema: str = "controladoria") -> str:
    """
    Lê o parquet RAW receita_doc_imanager_YYYY-MM-DD.parquet,
    mapeia ids do iManager -> ids do DW (id_regional, id_cidade, id_tempo),
    e salva em data/processed/fato_receita_doc_preparado.parquet.

    Saída (modelo):
      id_tempo, id_regional, id_cidade, tipo_documento, qtd_docs, valor_total
    """
    logger = get_run_logger()

    root = _project_root()
    raw_dir = root / "data" / "raw"
    processed_dir = root / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    caminho_raw = _encontrar_ultimo_raw(raw_dir)

    # Extrai data_ref do nome do arquivo: receita_doc_imanager_YYYY-MM-DD.parquet
    data_ref = caminho_raw.stem.replace("receita_doc_imanager_", "")
    datetime.strptime(data_ref, "%Y-%m-%d")  # valida formato

    saida = processed_dir / "fato_receita_doc_preparado.parquet"

    logger.info(f"Lendo RAW: {caminho_raw}")
    df = pd.read_parquet(caminho_raw)
    logger.info(f"Linhas RAW: {len(df):,}")

    if df.empty:
        logger.warning("RAW vazio. Salvando fato vazio.")
        pd.DataFrame().to_parquet(saida, index=False)
        return str(saida)

    # ---------------------------
    # Normaliza/valida colunas mínimas
    # ---------------------------
    # Ajuste se seus nomes vieram com aspas/maiúsculas
    rename_map = {
        "Faturamento": "data",
        "T_doc": "tipo_documento",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    obrigatorias = [
        "id_regional_imanager",
        "id_cidade_imanager",
        "data",
        "tipo_documento",
        "qtd_docs",
        "valor_total",
    ]
    faltantes = [c for c in obrigatorias if c not in df.columns]
    if faltantes:
        raise ValueError(f"RAW sem colunas obrigatórias: {faltantes}. Colunas atuais: {list(df.columns)}")

    # data mensal (primeiro dia do mês)
    df["data"] = pd.to_datetime(df["data"], errors="coerce").dt.date
    df = df.dropna(subset=["data"]).copy()

    # ids imanager como inteiros (com NA)
    df["id_regional_imanager"] = pd.to_numeric(df["id_regional_imanager"], errors="coerce").astype("Int64")
    df["id_cidade_imanager"] = pd.to_numeric(df["id_cidade_imanager"], errors="coerce").astype("Int64")

    # tipo_doc
    df["tipo_documento"] = df["tipo_documento"].astype(str).str.upper().str.strip()
    df.loc[~df["tipo_documento"].isin(["CPF", "CNPJ"]), "tipo_documento"] = "CPF"

    # métricas
    df["qtd_docs"] = pd.to_numeric(df["qtd_docs"], errors="coerce").fillna(0).astype("int64")
    df["valor_total"] = pd.to_numeric(df["valor_total"], errors="coerce").fillna(0.0)

    # ---------------------------
    # Conecta no DW para ler dims
    # ---------------------------
    cfg = get_db_config("dw")
    logger.info(f"Conectando DW: host={cfg.get('host')} db={cfg.get('database')} schema={schema}")
    conn = psycopg2.connect(**cfg)

    try:
        cols_reg = _get_columns(conn, schema, "dim_regionais")
        cols_cid = _get_columns(conn, schema, "dim_cidades")
        cols_tmp = _get_columns(conn, schema, "dim_tempo")

        pk_reg = _detectar_coluna_existente(cols_reg, ["id_regional", "id"], f"{schema}.dim_regionais")
        fk_reg_im = _detectar_coluna_existente(cols_reg, ["id_regional_imanager"], f"{schema}.dim_regionais")

        pk_cid = _detectar_coluna_existente(cols_cid, ["id_cidade", "id"], f"{schema}.dim_cidades")
        fk_cid_im = _detectar_coluna_existente(cols_cid, ["id_cidade_imanager"], f"{schema}.dim_cidades")

        pk_tmp = _detectar_coluna_existente(cols_tmp, ["id_tempo", "id"], f"{schema}.dim_tempo")
        col_data_tmp = _detectar_coluna_existente(cols_tmp, ["data", "dt", "data_dia", "data_calendario"], f"{schema}.dim_tempo")

        dim_regionais = pd.read_sql_query(
            f"""
            SELECT {pk_reg} AS id_regional,
                   {fk_reg_im} AS id_regional_imanager
            FROM {schema}.dim_regionais
            WHERE {fk_reg_im} IS NOT NULL
            """,
            conn,
        )

        dim_cidades = pd.read_sql_query(
            f"""
            SELECT {pk_cid} AS id_cidade,
                   {fk_cid_im} AS id_cidade_imanager
            FROM {schema}.dim_cidades
            WHERE {fk_cid_im} IS NOT NULL
            """,
            conn,
        )

        dim_tempo = pd.read_sql_query(
            f"""
            SELECT {pk_tmp} AS id_tempo,
                   {col_data_tmp} AS data
            FROM {schema}.dim_tempo
            """,
            conn,
        )
    finally:
        conn.close()

    logger.info(f"dim_regionais: {len(dim_regionais):,} | dim_cidades: {len(dim_cidades):,} | dim_tempo: {len(dim_tempo):,}")

    # tipos
    dim_regionais["id_regional_imanager"] = pd.to_numeric(dim_regionais["id_regional_imanager"], errors="coerce").astype("Int64")
    dim_cidades["id_cidade_imanager"] = pd.to_numeric(dim_cidades["id_cidade_imanager"], errors="coerce").astype("Int64")
    dim_tempo["data"] = pd.to_datetime(dim_tempo["data"], errors="coerce").dt.date

    # ---------------------------
    # Mapeia dimensões
    # ---------------------------
    df = df.merge(dim_regionais, on="id_regional_imanager", how="left", validate="m:1")
    df = df.merge(dim_cidades, on="id_cidade_imanager", how="left", validate="m:1")
    df = df.merge(dim_tempo, on="data", how="left", validate="m:1")

    # logs de não mapeados
    falt_reg = df[df["id_regional"].isna()]
    falt_cid = df[df["id_cidade"].isna()]
    falt_tmp = df[df["id_tempo"].isna()]

    logger.warning(
        f"Chaves não mapeadas -> regionais: {falt_reg.shape[0]:,} | cidades: {falt_cid.shape[0]:,} | tempo: {falt_tmp.shape[0]:,}"
    )

    if not falt_reg.empty:
        resumo = (
            falt_reg.groupby(["id_regional_imanager"], dropna=False)
            .size().reset_index(name="registros")
            .sort_values("registros", ascending=False)
        )
        logger.warning("Regionais não mapeadas (id_regional_imanager):")
        for _, r in resumo.head(50).iterrows():
            logger.warning(f"  id_regional_imanager={r['id_regional_imanager']} | registros={int(r['registros'])}")

    if not falt_cid.empty:
        resumo = (
            falt_cid.groupby(["id_cidade_imanager"], dropna=False)
            .size().reset_index(name="registros")
            .sort_values("registros", ascending=False)
        )
        logger.warning("Cidades não mapeadas (id_cidade_imanager):")
        for _, r in resumo.head(50).iterrows():
            logger.warning(f"  id_cidade_imanager={r['id_cidade_imanager']} | registros={int(r['registros'])}")

    if not falt_tmp.empty:
        resumo = (
            falt_tmp.groupby(["data"], dropna=False)
            .size().reset_index(name="registros")
            .sort_values("registros", ascending=False)
        )
        logger.warning("Datas não mapeadas (data -> dim_tempo):")
        for _, r in resumo.head(50).iterrows():
            logger.warning(f"  data={r['data']} | registros={int(r['registros'])}")

    if falt_reg.empty and falt_cid.empty and falt_tmp.empty:
        logger.info("Todas as chaves mapeadas com sucesso (id_regional, id_cidade, id_tempo).")

    # ---------------------------
    # Modelo final (fato)
    # ---------------------------
    df_fato = df[
        ["id_tempo", "id_regional", "id_cidade", "tipo_documento", "qtd_docs", "valor_total"]
    ].copy()

    for c in ["id_tempo", "id_regional", "id_cidade"]:
        df_fato[c] = pd.to_numeric(df_fato[c], errors="coerce").astype("Int64")

    df_fato.to_parquet(saida, index=False)
    logger.info(f"Fato receita_doc salvo em: {saida} | Linhas: {len(df_fato):,}")

    return str(saida)
