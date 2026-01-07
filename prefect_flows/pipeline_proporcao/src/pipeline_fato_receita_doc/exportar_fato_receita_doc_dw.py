from prefect import task
from pathlib import Path
import os
import time
import shutil

import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from utils import get_db_config

# Base do projeto (.../pipeline_proporcao)
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
PROCESSED_DIR = DATA_DIR / "processed"


@task
def exportar_fato_receita_doc_dw(lote: int = 5000):
    """
    Exporta fato_receita_doc_preparado.parquet para o DW (Postgres),
    garantindo criação de partições por ano (id_tempo) e limpando dados intermediários ao final.
    """
    load_dotenv()

    caminho_fato = PROCESSED_DIR / "fato_receita_doc_preparado.parquet"
    print(f"[exportar_fato_receita_doc_dw] Arquivo de entrada: {caminho_fato}")

    if not caminho_fato.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho_fato}")

    df = pd.read_parquet(caminho_fato)
    print("[exportar_fato_receita_doc_dw] Colunas disponíveis:", list(df.columns))

    # =========================================================
    # 1) COLUNAS ESPERADAS (EXATAMENTE como no DW)
    # =========================================================
    colunas_esperadas = [
        "id_tempo",
        "id_regional",
        "id_cidade",
        "tipo_documento",
        "qtd_docs",
        "valor_total",
    ]

    faltantes = [c for c in colunas_esperadas if c not in df.columns]
    if faltantes:
        raise ValueError(
            f"[exportar_fato_receita_doc_dw] Colunas ausentes no DataFrame: {faltantes}\n"
            f"Colunas atuais: {list(df.columns)}"
        )

    df = df[colunas_esperadas].copy()

    # =========================================================
    # 2) TIPAGEM FORTE + evita numpy.int64 no psycopg2
    # =========================================================
    chaves_int = ["id_tempo", "id_regional", "id_cidade", "qtd_docs"]

    for c in chaves_int:
        df[c] = pd.to_numeric(df[c], errors="raise").astype("int64")

    df["tipo_documento"] = df["tipo_documento"].astype(str)
    df["valor_total"] = pd.to_numeric(df["valor_total"], errors="raise").astype(float)

    # Validação de nulos
    nulos = df[chaves_int + ["tipo_documento", "valor_total"]].isna().sum()
    print("[exportar_fato_receita_doc_dw] Nulos nas colunas críticas:")
    print(nulos)

    if (nulos > 0).any():
        raise ValueError("[exportar_fato_receita_doc_dw] Existem valores nulos em colunas críticas.")

    if df.empty:
        print("[exportar_fato_receita_doc_dw] DataFrame vazio. Nada a carregar.")
        return 0

    # =========================================================
    # 3) ANOS PARA PARTIÇÃO (id_tempo no formato YYYYMMDD)
    # =========================================================
    anos = sorted({int(str(x)[:4]) for x in df["id_tempo"].unique()})
    print("[exportar_fato_receita_doc_dw] Anos encontrados na carga:", anos)

    # =========================================================
    # 4) CONEXÃO DW (garantir dbname)
    # =========================================================
    cfg_dw = get_db_config("dw")

    # psycopg2 usa "dbname"
    if "dbname" not in cfg_dw and "database" in cfg_dw:
        cfg_dw["dbname"] = cfg_dw.pop("database")

    if not cfg_dw.get("dbname"):
        raise ValueError(
            "[exportar_fato_receita_doc_dw] Config do DW sem dbname. "
            "Ajuste o .env / get_db_config para preencher o nome do banco."
        )

    pg_schema = os.getenv("PG_SCHEMA", "controladoria")
    tabela_alvo = f"{pg_schema}.fato_receita_doc"

    def py(v):
        """Converte numpy types -> Python types (resolve 'can't adapt numpy.int64')."""
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return None
        if isinstance(v, np.generic):
            return v.item()
        return v

    conn = psycopg2.connect(**cfg_dw)

    try:
        # Confirma DB real conectado
        with conn.cursor() as cur:
            cur.execute("SELECT current_database();")
            db_atual = cur.fetchone()[0]
        print(f"[exportar_fato_receita_doc_dw] ✅ Conectado no database: {db_atual}")
        print(f"[exportar_fato_receita_doc_dw] Schema alvo: {pg_schema}")
        print(f"[exportar_fato_receita_doc_dw] Tabela alvo: {tabela_alvo}")

        # =========================================================
        # 5) CRIAR/VERIFICAR PARTIÇÕES (igual ao modelo)
        # =========================================================
        with conn.cursor() as cur:
            for ano in anos:
                inicio = int(f"{ano}0101")
                fim = int(f"{ano + 1}0101")

                sql_part = f"""
                    CREATE TABLE IF NOT EXISTS {pg_schema}.fato_receita_doc_{ano}
                    PARTITION OF {pg_schema}.fato_receita_doc
                    FOR VALUES FROM (%s) TO (%s);
                """
                cur.execute(sql_part, (inicio, fim))

        conn.commit()
        print("[exportar_fato_receita_doc_dw] Partições verificadas/criadas com sucesso.")

        # =========================================================
        # 6) INSERT EM LOTES
        # =========================================================
        total = len(df)
        print(f"[exportar_fato_receita_doc_dw] Inserindo {total:,} registros em lotes de {lote}...")

        sql_insert = f"""
            INSERT INTO {tabela_alvo} (
                id_tempo,
                id_regional,
                id_cidade,
                tipo_documento,
                qtd_docs,
                valor_total
            )
            VALUES %s
        """

        inicio_total = time.time()
        inseridos = 0

        with conn.cursor() as cur:
            for i in range(0, total, lote):
                chunk = df.iloc[i : i + lote]

                valores = [
                    tuple(py(x) for x in row)
                    for row in chunk.itertuples(index=False, name=None)
                ]

                execute_values(
                    cur,
                    sql_insert,
                    valores,
                    page_size=min(lote, 10000),
                )
                conn.commit()

                inseridos += len(valores)
                print(f"[exportar_fato_receita_doc_dw] Lote {i // lote + 1}: {len(valores)} linhas inseridas.")

        dur = time.time() - inicio_total
        print(f"[exportar_fato_receita_doc_dw] ✅ Carga concluída: {inseridos:,} linhas em {dur:.2f}s.")
        return inseridos

    except Exception as e:
        conn.rollback()
        print("[exportar_fato_receita_doc_dw] Erro durante a carga. Rollback executado.")
        raise e

    finally:
        conn.close()
        print("[exportar_fato_receita_doc_dw] Conexão fechada.")

        # =========================================================
        # 7) LIMPEZA FINAL AUTOMÁTICA (igual aos outros processos)
        # =========================================================
        print("[exportar_fato_receita_doc_dw] Limpando pastas de dados...")

        pastas_para_limpar = [
            DATA_DIR / "processed",
            DATA_DIR / "raw",
            DATA_DIR / "staging",
        ]

        for pasta in pastas_para_limpar:
            if pasta.exists():
                for item in pasta.iterdir():
                    try:
                        if item.is_file():
                            item.unlink()
                        elif item.is_dir():
                            shutil.rmtree(item)
                    except Exception as ex:
                        print(f"[WARN] Erro ao remover {item}: {ex}")

        print("[exportar_fato_receita_doc_dw] Pastas limpas com sucesso.")
