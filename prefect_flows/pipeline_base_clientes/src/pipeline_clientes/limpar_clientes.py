from prefect import task
from prefect.logging import get_run_logger

import pandas as pd
import unicodedata
import re
from pathlib import Path
from datetime import datetime


def limpar_texto(txt):
    if pd.isna(txt):
        return txt
    txt = unicodedata.normalize("NFKD", str(txt))
    txt = "".join(c for c in txt if not unicodedata.combining(c))
    txt = re.sub(r"[^a-zA-Z0-9\s]", "", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt.upper()


def _project_root() -> Path:
    # arquivo está em: <raiz>/src/pipeline_clientes/limpar_clientes.py
    return Path(__file__).resolve().parents[2]


def _encontrar_ultimo_raw(raw_dir: Path) -> Path:
    arquivos = sorted(raw_dir.glob("clientes_imanager_*.parquet"))
    if not arquivos:
        raise FileNotFoundError(
            f"Nenhum arquivo encontrado em: {raw_dir} (padrão clientes_imanager_*.parquet)"
        )
    return arquivos[-1]


@task(name="limpar_clientes")
def limpar_clientes(aplicar_dedup: bool = True) -> str:
    """
    Lê o parquet raw mais recente, aplica limpeza/transformações e salva em data/staging.

    Logs:
      - linhas do arquivo raw
      - linhas removidas por dedup (se ativado)
      - efeito do pivot (mudança de granularidade)
      - linhas removidas por filtros (regional/cidade)
      - linhas finais do staging
    """
    logger = get_run_logger()

    root = _project_root()
    raw_dir = root / "data" / "raw"
    staging_dir = root / "data" / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)

    caminho_entrada = _encontrar_ultimo_raw(raw_dir)

    # Extrai data_ref do nome do arquivo: clientes_imanager_YYYY-MM-DD.parquet
    data_ref = caminho_entrada.stem.replace("clientes_imanager_", "")
    datetime.strptime(data_ref, "%Y-%m-%d")  

    caminho_saida = staging_dir / f"clientes_limpos_{data_ref}.parquet"

    logger.info(f"Carregando arquivo raw: {caminho_entrada}")
    df = pd.read_parquet(caminho_entrada)

    linhas_raw = len(df)
    logger.info(f"Linhas no raw (entrada): {linhas_raw:,}")

    # schema esperado no staging (inclui id_regional_imanager)
    colunas_saida = [
        "data",
        "id_regional_imanager",  
        "regional",
        "id_cidade_imanager",
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

    if df.empty:
        logger.warning("Sem dados no período. Gerando arquivo de saída vazio com schema esperado.")
        pd.DataFrame(columns=colunas_saida).to_parquet(caminho_saida, index=False)
        logger.info(f"Arquivo staging (vazio) salvo em: {caminho_saida}")
        return str(caminho_saida)

    # garante que a coluna existe
    if "id_regional_imanager" not in df.columns:
        raise KeyError(
            "Coluna 'id_regional_imanager' não encontrada no raw. "
            "Confirme se a extração já está trazendo esse campo."
        )

    # Dedup opcional no raw (normalmente não altera, mas mantém padrão)
    if aplicar_dedup:
        antes = len(df)
        df = df.drop_duplicates()
        depois = len(df)
        logger.info(f"Dedup raw: removidas {antes - depois:,} linhas duplicadas (raw).")

    # -------------------------
    # Pivot: situações -> colunas
    # -------------------------
    linhas_pre_pivot = len(df)

    df_expandido = df.pivot_table(
        index=["data_posicao", "id_regional_imanager", "regional","id_cidade_imanager", "cidade", "tipo_documento"],
        columns="descricaosituacaocontrato",
        values="contratos",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()

    linhas_pos_pivot = len(df_expandido)
    logger.info(
        f"Transformação (pivot): {linhas_pre_pivot:,} linhas (raw) -> {linhas_pos_pivot:,} linhas (granularidade final)"
    )

    # Garantir colunas esperadas (situações)
    situacoes_esperadas = [
        "Aguardando Conexão",
        "Cancelado",
        "Conectado/Ativo",
        "Conectado/Inadimplente Parcial",
        "Conectado/Inadimplente Total",
        "Inadimplente",
        "Pausado",
    ]
    for s in situacoes_esperadas:
        if s not in df_expandido.columns:
            df_expandido[s] = 0

    # -------------------------
    # Filtros de regional/cidade
    # -------------------------
    linhas_antes_filtros = len(df_expandido)

    regionais_excluir = ["ALUGUEIS", "AXIS", "INATIVO"]
    df_expandido = df_expandido[~df_expandido["regional"].isin(regionais_excluir)]

    cidades_excluir = ["ALUGUEIS", "AXIS TELECOM", "SAO TOME DAS LETRAS"]
    df_expandido = df_expandido[~df_expandido["cidade"].isin(cidades_excluir)]

    linhas_depois_filtros = len(df_expandido)
    removidas_por_filtros = linhas_antes_filtros - linhas_depois_filtros
    logger.info(f"Removidas por filtros (regional/cidade): {removidas_por_filtros:,}")

    # -------------------------
    # Renomear colunas para padrão do DW
    # -------------------------
    mapa_colunas = {
        "data_posicao": "data",
        "regional": "regional",
        "cidade": "cidade",
        "tipo_documento": "tipo_documento",
        "Aguardando Conexão": "aguardando_conexao",
        "Cancelado": "cancelado",
        "Conectado/Ativo": "conectado_ativo",
        "Conectado/Inadimplente Parcial": "conectado_inadimplente_parcial",
        "Conectado/Inadimplente Total": "conectado_inadimplente_total",
        "Inadimplente": "inadimplente",
        "Pausado": "pausado",
    }
    df_expandido = df_expandido.rename(columns=mapa_colunas)

    # -------------------------
    # Métricas derivadas
    # -------------------------
    df_expandido["total_clientes_ativos"] = (
        df_expandido["conectado_ativo"]
        + df_expandido["conectado_inadimplente_parcial"]
        + df_expandido["conectado_inadimplente_total"]
        + df_expandido["inadimplente"]
    )

    df_expandido["qtde_contratos"] = (
        df_expandido["aguardando_conexao"]
        + df_expandido["cancelado"]
        + df_expandido["conectado_ativo"]
        + df_expandido["conectado_inadimplente_parcial"]
        + df_expandido["conectado_inadimplente_total"]
        + df_expandido["inadimplente"]
        + df_expandido["pausado"]
    )

    # Força a saída no padrão e ordem corretos
    df_expandido = df_expandido[colunas_saida]

    linhas_finais = len(df_expandido)
    logger.info(f"Linhas finais (staging): {linhas_finais:,}")
    logger.info(f"Total removidas por limpeza/filtros: {removidas_por_filtros:,}")

    df_expandido.to_parquet(caminho_saida, index=False)
    logger.info(f"Arquivo staging salvo em: {caminho_saida}")

    return str(caminho_saida)
