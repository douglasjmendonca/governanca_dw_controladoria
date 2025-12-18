from prefect import task
from pathlib import Path
import time
import shutil

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from utils import get_db_config


# Base do projeto
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
PROCESSED_DIR = DATA_DIR / "processed"


def _encontrar_fato_processado(processed_dir: Path) -> Path:
    """
    Prioriza fato_clientes_preparado.parquet.
    Se não existir, pega o mais recente no padrão fato_clientes_YYYY-MM-DD.parquet.
    """
    preferido = processed_dir / "fato_clientes_preparado.parquet"
    if preferido.exists():
        return preferido

    arquivos = sorted(processed_dir.glob("fato_clientes_*.parquet"))
    # remove o próprio "preparado" caso tenha sido capturado no glob
    arquivos = [a for a in arquivos if a.name != "fato_clientes_preparado.parquet"]

    if not arquivos:
        raise FileNotFoundError(
            f"Nenhum parquet encontrado em {processed_dir}. "
            "Esperado: fato_clientes_preparado.parquet OU fato_clientes_YYYY-MM-DD.parquet"
        )
    return arquivos[-1]


def _garantir_particionamento_id_tempo(conn, schema: str, table: str) -> None:
    """
    Garante que a tabela está particionada por RANGE(id_tempo).
    """
    with conn.cursor() as cur:
        cur.execute("SELECT pg_get_partkeydef(%s::regclass);", (f"{schema}.{table}",))
        partdef = cur.fetchone()

    if not partdef or not partdef[0]:
        raise ValueError(
            f"[carregar_fato_clientes] A tabela {schema}.{table} não parece ser particionada."
        )

    partdef_txt = partdef[0].strip().upper()

    # Ex.: "RANGE (id_tempo)"
    if "RANGE" not in partdef_txt or "ID_TEMPO" not in partdef_txt:
        raise ValueError(
            f"[carregar_fato_clientes] Sua tabela {schema}.{table} NÃO está particionada por id_tempo.\n"
            f"Partição atual: {partdef[0]}\n"
            f"✅ Recrie a tabela com: PARTITION BY RANGE (id_tempo) (igual a fato_dre)."
        )


@task(name="carregar_fato_clientes")
def carregar_fato_clientes(lote: int = 5000):
    # =========================
    # 1) Ler parquet (processed)
    # =========================
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    caminho_fato = _encontrar_fato_processado(PROCESSED_DIR)

    print(f"[carregar_fato_clientes] Arquivo de entrada: {caminho_fato}")

    df = pd.read_parquet(caminho_fato)

    colunas_esperadas = [
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

    print("[carregar_fato_clientes] Colunas disponíveis:", list(df.columns))

    faltantes = [c for c in colunas_esperadas if c not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas ausentes no DataFrame: {faltantes}")

    df = df[colunas_esperadas].copy()

    # =========================
    # 2) Tipagem / validações
    # =========================
    chaves_int = ["id_tempo", "id_regional", "id_cidade"]

    metricas_int = [
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

    for c in chaves_int:
        df[c] = pd.to_numeric(df[c], errors="raise").astype("int64")

    df["tipo_documento"] = df["tipo_documento"].astype(str).str.strip().str.upper()
    tipos_validos = {"CPF", "CNPJ"}
    tipos_invalidos = sorted(set(df["tipo_documento"].unique()) - tipos_validos)
    if tipos_invalidos:
        raise ValueError(
            f"[carregar_fato_clientes] tipo_documento inválido encontrado: {tipos_invalidos}. "
            "Esperado apenas 'CPF' ou 'CNPJ'."
        )

    for c in metricas_int:
        df[c] = pd.to_numeric(df[c], errors="raise").fillna(0).astype("int64")

    nulos = df[chaves_int].isna().sum()
    print("[carregar_fato_clientes] Nulos nas chaves:")
    print(nulos)

    if (nulos > 0).any():
        raise ValueError("[carregar_fato_clientes] Existem chaves nulas na fato.")

    if df.empty:
        print("[carregar_fato_clientes] DataFrame vazio. Nada a carregar.")
        return

    # =========================
    # 3) Partições por id_tempo
    # =========================
    anos = sorted({int(str(x)[:4]) for x in df["id_tempo"].unique()})
    print("[carregar_fato_clientes] Anos encontrados na carga:", anos)

    cfg_dw = get_db_config("dw")

    # >>> FORÇA schema correto <<<
    pg_schema = "controladoria"
    tabela_alvo = f"{pg_schema}.fato_clientes"

    def py(v):
        if v is None or pd.isna(v):
            return None
        return v

    conn = psycopg2.connect(**cfg_dw)

    try:
        # 0) valida particionamento correto (RANGE(id_tempo))
        _garantir_particionamento_id_tempo(conn, pg_schema, "fato_clientes")

        # 1) Garante partições anuais (id_tempo é INT tipo 20250101)
        with conn.cursor() as cur:
            for ano in anos:
                inicio = int(f"{ano}0101")
                fim = int(f"{ano + 1}0101")

                sql_part = f"""
                    CREATE TABLE IF NOT EXISTS {pg_schema}.fato_clientes_{ano}
                    PARTITION OF {pg_schema}.fato_clientes
                    FOR VALUES FROM (%s) TO (%s);
                """
                cur.execute(sql_part, (inicio, fim))

        conn.commit()
        print("[carregar_fato_clientes] Partições verificadas/criadas.")

        # 2) Insere em lotes
        total = len(df)
        print(
            f"[carregar_fato_clientes] Inserindo {total:,} registros "
            f"em lotes de {lote}..."
        )

        sql_insert = f"""
            INSERT INTO {tabela_alvo} (
                id_tempo,
                id_regional,
                id_cidade,
                tipo_documento,
                qtde_contratos,
                aguardando_conexao,
                cancelado,
                conectado_ativo,
                conectado_inadimplente_parcial,
                conectado_inadimplente_total,
                inadimplente,
                pausado,
                total_clientes_ativos
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
                    f"[carregar_fato_clientes] Lote {i // lote + 1}: "
                    f"{len(valores)} linhas inseridas."
                )

        dur = time.time() - inicio_total
        print(
            f"[carregar_fato_clientes] Carga concluída: "
            f"{inseridos:,} linhas em {dur:.2f}s."
        )

    except Exception as e:
        conn.rollback()
        print("[carregar_fato_clientes] Erro durante a carga. Rollback executado.")
        raise e

    finally:
        conn.close()
        print("[carregar_fato_clientes] Conexão fechada.")

        # ==============================
        # LIMPEZA FINAL AUTOMÁTICA
        # ==============================
        print("[carregar_fato_clientes] Limpando pastas de dados...")

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
                    except Exception as e:
                        print(f"[WARN] Erro ao remover {item}: {e}")

        print("[carregar_fato_clientes] Pastas limpas com sucesso")
