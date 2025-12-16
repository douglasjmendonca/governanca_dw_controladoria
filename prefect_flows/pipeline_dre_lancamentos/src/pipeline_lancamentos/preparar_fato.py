from prefect import task
import pandas as pd
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

from utils import get_db_config

# Estrutura padrão do projeto
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
PROCESSED_DIR = DATA_DIR / "processed"


def _normalize(s: object) -> str:
    if pd.isna(s):
        return ""
    return " ".join(str(s).strip().upper().split())


@task
def preparar_fato():
    load_dotenv()

    caminho_entrada = PROCESSED_DIR / "dre_classificado.parquet"
    caminho_saida = PROCESSED_DIR / "fato_dre_preparado.parquet"

    print(f"[preparar_fato] Arquivo de entrada: {caminho_entrada}")
    print(f"[preparar_fato] Arquivo de saída:  {caminho_saida}")

    if not caminho_entrada.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho_entrada}")

    # =========================
    # Carrega DRE classificado
    # =========================
    df = pd.read_parquet(caminho_entrada)

    print("[preparar_fato] Colunas disponíveis:", list(df.columns))
    print("[preparar_fato] dtype DATA (antes):", df["DATA"].dtype)

    # Garante que DATA está em datetime
    df["DATA"] = pd.to_datetime(df["DATA"])
    print("[preparar_fato] dtype DATA (depois):", df["DATA"].dtype)

    # Normalização dos textos para cruzar com dimensões
    df["item_norm"] = df["item"].map(_normalize)
    df["natureza_norm"] = df["natureza"].map(_normalize)
    df["detalhe_norm"] = df["detalhe"].map(_normalize)
    df["conta_norm"] = df["conta"].map(_normalize)

    # =========================
    # Conexão com o DW
    # =========================
    conn = psycopg2.connect(**get_db_config("dw"))
    try:
        dim_itens = pd.read_sql(
            "SELECT id_item, item FROM controladoria.dim_itens;", conn
        )
        dim_naturezas = pd.read_sql(
            "SELECT id_natureza, natureza FROM controladoria.dim_naturezas;", conn
        )
        dim_detalhes = pd.read_sql(
            "SELECT id_detalhe, detalhe FROM controladoria.dim_detalhes;", conn
        )
        dim_contas = pd.read_sql(
            "SELECT id_conta, conta FROM controladoria.dim_contas;", conn
        )
        dim_tempo = pd.read_sql(
            "SELECT id_tempo, data FROM controladoria.dim_tempo;", conn
        )
    finally:
        conn.close()

    # =========================
    # Normalização das dimensões textuais
    # =========================
    dim_itens["item_norm"] = dim_itens["item"].map(_normalize)
    dim_naturezas["natureza_norm"] = dim_naturezas["natureza"].map(_normalize)
    dim_detalhes["detalhe_norm"] = dim_detalhes["detalhe"].map(_normalize)
    dim_contas["conta_norm"] = dim_contas["conta"].map(_normalize)

    # =========================
    # Joins por texto
    # =========================
    df = df.merge(dim_itens[["id_item", "item_norm"]], on="item_norm", how="left")
    df = df.merge(
        dim_naturezas[["id_natureza", "natureza_norm"]],
        on="natureza_norm",
        how="left",
    )
    df = df.merge(
        dim_detalhes[["id_detalhe", "detalhe_norm"]],
        on="detalhe_norm",
        how="left",
    )
    df = df.merge(dim_contas[["id_conta", "conta_norm"]], on="conta_norm", how="left")

    # =========================
    # Join com dim_tempo (ajuste de tipo)
    # =========================
    print("[preparar_fato] dtype dim_tempo['data'] (antes):", dim_tempo["data"].dtype)
    dim_tempo["data"] = pd.to_datetime(dim_tempo["data"])
    print("[preparar_fato] dtype dim_tempo['data'] (depois):", dim_tempo["data"].dtype)

    df = df.merge(
        dim_tempo[["id_tempo, data".split(", ")[0], "data"]],
        left_on="DATA",
        right_on="data",
        how="left",
    ).drop(columns=["data"])

    # =========================
    # Padronização final da medida
    # =========================
    df = df.rename(columns={"VALORG": "valor"})

    # =========================
    # Montagem final da fato
    # =========================
    colunas_fato = [
        "id_tempo",
        "id_item",
        "id_natureza",
        "id_detalhe",
        "id_conta",
        "id_regcid",
        "valor",
    ]

    fato = df[colunas_fato].copy()

    print("[preparar_fato] Linhas na fato:", len(fato))
    print("[preparar_fato] Nulos por coluna:")
    print(fato.isna().sum())

    # =========================
    # Persistência final
    # =========================
    fato.to_parquet(caminho_saida, index=False)
    print(f"[preparar_fato] Fato preparada salva em: {caminho_saida}")
