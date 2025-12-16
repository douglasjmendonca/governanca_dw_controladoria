from prefect import task
from pathlib import Path
import os
import time
import shutil

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from utils import get_db_config

# Base do projeto
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
PROCESSED_DIR = DATA_DIR / "processed"


@task
def carregar_fato_dre(lote: int = 5000):
    load_dotenv()

    caminho_fato = PROCESSED_DIR / "fato_dre_preparado.parquet"
    print(f"[carregar_fato_dre] Arquivo de entrada: {caminho_fato}")

    if not caminho_fato.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho_fato}")

    df = pd.read_parquet(caminho_fato)

    # Garantia do nome da coluna de valor
    if "VALORG" in df.columns and "valor" not in df.columns:
        df = df.rename(columns={"VALORG": "valor"})

    colunas_esperadas = [
        "id_tempo",
        "id_item",
        "id_natureza",
        "id_detalhe",
        "id_conta",
        "id_regcid",
        "valor",
    ]

    print("[carregar_fato_dre] Colunas disponíveis:", list(df.columns))

    faltantes = [c for c in colunas_esperadas if c not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas ausentes no DataFrame: {faltantes}")

    df = df[colunas_esperadas].copy()

    # Tipagem
    chaves = [
        "id_tempo",
        "id_item",
        "id_natureza",
        "id_detalhe",
        "id_conta",
        "id_regcid",
    ]

    for c in chaves:
        df[c] = pd.to_numeric(df[c], errors="raise").astype("int64")

    df["valor"] = pd.to_numeric(df["valor"], errors="raise").astype(float)

    # Validação de nulos
    nulos = df[chaves].isna().sum()
    print("[carregar_fato_dre] Nulos nas chaves:")
    print(nulos)

    if (nulos > 0).any():
        raise ValueError(
            "[carregar_fato_dre] Existem chaves nulas na fato."
        )

    if df.empty:
        print("[carregar_fato_dre] DataFrame vazio. Nada a carregar.")
        return

    # Anos para partição
    anos = sorted({int(str(x)[:4]) for x in df["id_tempo"].unique()})
    print("[carregar_fato_dre] Anos encontrados na carga:", anos)

    cfg_dw = get_db_config("dw")
    pg_schema = os.getenv("PG_SCHEMA", "controladoria")
    tabela_alvo = f"{pg_schema}.fato_dre"

    def py(v):
        if v is None or pd.isna(v):
            return None
        return v

    conn = psycopg2.connect(**cfg_dw)

    try:
        with conn.cursor() as cur:
            for ano in anos:
                inicio = int(f"{ano}0101")
                fim = int(f"{ano + 1}0101")

                sql_part = f"""
                    CREATE TABLE IF NOT EXISTS {pg_schema}.fato_dre_{ano}
                    PARTITION OF {pg_schema}.fato_dre
                    FOR VALUES FROM (%s) TO (%s);
                """
                cur.execute(sql_part, (inicio, fim))

        conn.commit()
        print("[carregar_fato_dre] Partições verificadas/criadas.")

        total = len(df)
        print(
            f"[carregar_fato_dre] Inserindo {total:,} registros "
            f"em lotes de {lote}..."
        )

        sql_insert = f"""
            INSERT INTO {tabela_alvo} (
                id_tempo,
                id_item,
                id_natureza,
                id_detalhe,
                id_conta,
                id_regcid,
                valor
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
                print(
                    f"[carregar_fato_dre] Lote {i // lote + 1}: "
                    f"{len(valores)} linhas inseridas."
                )

        dur = time.time() - inicio_total
        print(
            f"[carregar_fato_dre] Carga concluída: "
            f"{inseridos:,} linhas em {dur:.2f}s."
        )

    except Exception as e:
        conn.rollback()
        print("[carregar_fato_dre] Erro durante a carga. Rollback executado.")
        raise e

    finally:
        conn.close()
        print("[carregar_fato_dre] Conexão fechada.")

        # ==============================
        # LIMPEZA FINAL AUTOMÁTICA
        # ==============================
        print("[carregar_fato_dre] Limpando pastas de dados...")

        pastas_para_limpar = [
            DATA_DIR / "processed",
            DATA_DIR / "raw",
            DATA_DIR / "staging"
        ]

        for pasta in pastas_para_limpar:
            if pasta.exists():
                for item in pasta.iterdir():
                    try:
                        if item.is_file():
                            item.unlink()
                        elif item.is_dir():
                            shutil.rmtree(item)
                    except Exception as e:
                        print(f"[WARN] Erro ao remover {item}: {e}")

        print("[carregar_fato_dre] Pastas limpas com sucesso")
