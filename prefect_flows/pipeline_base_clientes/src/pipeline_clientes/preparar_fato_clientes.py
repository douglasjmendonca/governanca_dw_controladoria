# src/pipeline_clientes/preparar_fato_clientes.py

from prefect import task
from prefect.logging import get_run_logger

import pandas as pd
import psycopg2
from pathlib import Path
from datetime import datetime


from utils import get_db_config


def _project_root() -> Path:
    # arquivo está em: <raiz>/src/pipeline_clientes/preparar_fato_clientes.py
    return Path(__file__).resolve().parents[2]


def _encontrar_ultimo_staging(staging_dir: Path) -> Path:
    arquivos = sorted(staging_dir.glob("clientes_limpos_*.parquet"))
    if not arquivos:
        raise FileNotFoundError(
            f"Nenhum arquivo encontrado em: {staging_dir} (padrão clientes_limpos_*.parquet)"
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


@task(name="preparar_fato_clientes")
def preparar_fato_clientes(schema: str = "controladoria") -> str:
    """
    Lê o parquet staging mais recente (clientes_limpos_YYYY-MM-DD.parquet),
    mapeia as chaves do DW (id_regional, id_cidade, id_tempo),
    monta a tabela fato e salva em data/processed/fato_clientes_YYYY-MM-DD.parquet.

    Logs:
      - contagem dims
      - contagem de chaves não mapeadas (com detalhe do valor faltante)
    """
    logger = get_run_logger()

    root = _project_root()
    staging_dir = root / "data" / "staging"
    processed_dir = root / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    caminho_staging = _encontrar_ultimo_staging(staging_dir)

    # Extrai data_ref do nome do arquivo: clientes_limpos_YYYY-MM-DD.parquet
    data_ref = caminho_staging.stem.replace("clientes_limpos_", "")
    datetime.strptime(data_ref, "%Y-%m-%d")  # valida formato

    saida = processed_dir / f"fato_clientes_preparado.parquet"

    logger.info(f"Lendo staging: {caminho_staging}")
    df = pd.read_parquet(caminho_staging)
    logger.info(f"Linhas staging: {len(df):,}")

    if df.empty:
        logger.warning("Staging vazio. Gerando fato vazio (apenas schema) e salvando.")
        pd.DataFrame().to_parquet(saida, index=False)
        logger.info(f"Fato clientes salvo (vazio) em: {saida}")
        return str(saida)

    # ---------------------------
    # Valida colunas mínimas
    # ---------------------------
    obrigatorias = [
        "data",
        "id_regional_imanager",
        "id_cidade_imanager",
        "regional",
        "cidade",
        "tipo_documento",
        "aguardando_conexao",
        "cancelado",
        "conectado_ativo",
        "conectado_inadimplente_parcial",
        "conectado_inadimplente_total",
        "inadimplente",
        "pausado",
        "qtde_contratos",
        "total_clientes_ativos",
    ]
    faltantes = [c for c in obrigatorias if c not in df.columns]
    if faltantes:
        raise ValueError(f"Staging sem colunas obrigatórias: {faltantes}")

    # padroniza data
    df["data"] = pd.to_datetime(df["data"], errors="coerce").dt.date
    df = df.dropna(subset=["data"]).copy()

    # força ids imanager como inteiros (suporta NA)
    df["id_regional_imanager"] = pd.to_numeric(df["id_regional_imanager"], errors="coerce").astype("Int64")
    df["id_cidade_imanager"] = pd.to_numeric(df["id_cidade_imanager"], errors="coerce").astype("Int64")

    # ---------------------------
    # Conecta no DW (igual DRE)
    # ---------------------------
    cfg = get_db_config("dw")
    logger.info(
        f"Conectando DW: host={cfg.get('host')} db={cfg.get('database')} schema={schema}"
    )
    conn = psycopg2.connect(**cfg)

    try:
        # Descobrir colunas reais das dims (robusto p/ id vs id_* etc.)
        cols_reg = _get_columns(conn, schema, "dim_regionais")
        cols_cid = _get_columns(conn, schema, "dim_cidades")
        cols_tmp = _get_columns(conn, schema, "dim_tempo")

        pk_reg = _detectar_coluna_existente(cols_reg, ["id_regional", "id"], f"{schema}.dim_regionais")
        fk_reg_im = _detectar_coluna_existente(
            cols_reg, ["id_regional_imanager", "id_regional_imanage", "id_regional_imananger"],
            f"{schema}.dim_regionais"
        )

        pk_cid = _detectar_coluna_existente(cols_cid, ["id_cidade", "id"], f"{schema}.dim_cidades")
        fk_cid_im = _detectar_coluna_existente(
            cols_cid, ["id_cidade_imanager", "id_cidade_imanage", "id_cidade_imananger"],
            f"{schema}.dim_cidades"
        )

        pk_tmp = _detectar_coluna_existente(cols_tmp, ["id_tempo", "id"], f"{schema}.dim_tempo")
        col_data_tmp = _detectar_coluna_existente(
            cols_tmp, ["data", "dt", "data_dia", "data_calendario"],
            f"{schema}.dim_tempo"
        )

        # Carregar dims essenciais
        dim_regionais = pd.read_sql_query(
            f"""
            SELECT
                {pk_reg}   AS id_regional,
                {fk_reg_im} AS id_regional_imanager
            FROM {schema}.dim_regionais
            WHERE {fk_reg_im} IS NOT NULL
            """,
            conn,
        )
        dim_cidades = pd.read_sql_query(
            f"""
            SELECT
                {pk_cid}   AS id_cidade,
                {fk_cid_im} AS id_cidade_imanager
            FROM {schema}.dim_cidades
            WHERE {fk_cid_im} IS NOT NULL
            """,
            conn,
        )
        dim_tempo = pd.read_sql_query(
            f"""
            SELECT
                {pk_tmp}    AS id_tempo,
                {col_data_tmp} AS data
            FROM {schema}.dim_tempo
            """,
            conn,
        )
    finally:
        conn.close()

    logger.info(f"dim_regionais carregada: {len(dim_regionais):,}")
    logger.info(f"dim_cidades carregada: {len(dim_cidades):,}")
    logger.info(f"dim_tempo carregada: {len(dim_tempo):,}")

    # tipos
    dim_regionais["id_regional_imanager"] = pd.to_numeric(dim_regionais["id_regional_imanager"], errors="coerce").astype("Int64")
    dim_cidades["id_cidade_imanager"] = pd.to_numeric(dim_cidades["id_cidade_imanager"], errors="coerce").astype("Int64")
    dim_tempo["data"] = pd.to_datetime(dim_tempo["data"], errors="coerce").dt.date

    # ---------------------------
    # Mapeamentos
    # ---------------------------
    df = df.merge(
        dim_regionais,
        on="id_regional_imanager",
        how="left",
        validate="m:1"
    )
    df = df.merge(
        dim_cidades,
        on="id_cidade_imanager",
        how="left",
        validate="m:1"
    )
    df = df.merge(
        dim_tempo,
        on="data",
        how="left",
        validate="m:1"
    )

    # ---------------------------
    # Log de chaves não mapeadas (com valores!)
    # ---------------------------
    falt_reg = df[df["id_regional"].isna()]
    falt_cid = df[df["id_cidade"].isna()]
    falt_tmp = df[df["id_tempo"].isna()]

    logger.warning(
        f"Chaves não mapeadas -> regionais: {falt_reg.shape[0]:,} | cidades: {falt_cid.shape[0]:,} | tempo: {falt_tmp.shape[0]:,}"
    )

    if not falt_cid.empty:
        # mostra quais id_cidade_imanager e qual nome de cidade veio no staging
        resumo = (
            falt_cid.groupby(["id_cidade_imanager", "cidade"], dropna=False)
            .size()
            .reset_index(name="registros")
            .sort_values("registros", ascending=False)
        )
        logger.warning("Cidades não mapeadas (id_cidade_imanager -> dim_cidades):")
        for _, r in resumo.head(50).iterrows():
            logger.warning(
                f"  id_cidade_imanager={r['id_cidade_imanager']} | cidade='{r['cidade']}' | registros={int(r['registros'])}"
            )

    if not falt_reg.empty:
        resumo = (
            falt_reg.groupby(["id_regional_imanager", "regional"], dropna=False)
            .size()
            .reset_index(name="registros")
            .sort_values("registros", ascending=False)
        )
        logger.warning("Regionais não mapeadas (id_regional_imanager -> dim_regionais):")
        for _, r in resumo.head(50).iterrows():
            logger.warning(
                f"  id_regional_imanager={r['id_regional_imanager']} | regional='{r['regional']}' | registros={int(r['registros'])}"
            )

    if not falt_tmp.empty:
        resumo = (
            falt_tmp.groupby(["data"], dropna=False)
            .size()
            .reset_index(name="registros")
            .sort_values("registros", ascending=False)
        )
        logger.warning("Datas não mapeadas (data -> dim_tempo):")
        for _, r in resumo.head(50).iterrows():
            logger.warning(f"  data={r['data']} | registros={int(r['registros'])}")

    # Só loga “tudo ok” se REALMENTE estiver tudo ok
    if falt_reg.empty and falt_cid.empty and falt_tmp.empty:
        logger.info("Todas as chaves foram mapeadas com sucesso (id_regional, id_cidade, id_tempo).")

    # ---------------------------
    # Montar fato final (modelo)
    # ---------------------------
    df_fato = df[
        [
            "id_tempo",
            "id_regional",
            "id_cidade",
            "tipo_documento",
            "qtde_contratos",
            "aguardando_conexao",
            "cancelado",
            "conectado_ativo",
            "conectado_inadimplente_parcial",
            "conectado_inadimplente_total",
            "inadimplente",
            "pausado",
            "total_clientes_ativos",
        ]
    ].copy()

    # Inteiros com NA
    for c in ["id_tempo", "id_regional", "id_cidade"]:
        df_fato[c] = pd.to_numeric(df_fato[c], errors="coerce").astype("Int64")

    # numéricos como int (mantém integridade)
    num_cols = [
        "qtde_contratos",
        "aguardando_conexao",
        "cancelado",
        "conectado_ativo",
        "conectado_inadimplente_parcial",
        "conectado_inadimplente_total",
        "inadimplente",
        "pausado",
        "total_clientes_ativos",
    ]
    for c in num_cols:
        df_fato[c] = pd.to_numeric(df_fato[c], errors="coerce").fillna(0).astype("int64")

    # ---------------------------
    # Salvar em data/processed (modelo final)
    # ---------------------------
    df_fato.to_parquet(saida, index=False)
    logger.info(f"Fato clientes salvo em: {saida} | Linhas: {len(df_fato):,}")

    return str(saida)
