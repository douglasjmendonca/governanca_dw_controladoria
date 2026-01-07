from prefect import flow
from datetime import datetime

from pipeline_fato_receita_doc.imanager_extrair_receita_doc import extrair_receita_doc_imanager
from pipeline_fato_receita_doc.preparar_fato_receita_doc import preparar_fato_receita_doc
from pipeline_fato_receita_doc.exportar_fato_receita_doc_dw import exportar_fato_receita_doc_dw

@flow(name="pipeline_receita_doc")
def pipeline_receita_doc():
    print("Iniciando pipeline Receita por Tipo de Documento (CPF/CNPJ)...")

    data_ref = input("Digite a data (YYYY-MM-DD): ").strip()
    datetime.strptime(data_ref, "%Y-%m-%d")
    print(f"Data de referência: {data_ref}")

    # 1) RAW (extrai mês de data_ref e salva parquet)
    caminho_raw = extrair_receita_doc_imanager(data_ref=data_ref, db="imanager")
    print(f"✔ RAW salvo em: {caminho_raw}")

    # 2) PROCESSED (mapeia dims -> id_tempo/id_regional/id_cidade e gera parquet pronto)
    caminho_processed = preparar_fato_receita_doc(schema="controladoria")
    print(f"✔ PROCESSED salvo em: {caminho_processed}")

    # 3) DW (cria partição do ano se precisar e insere em lotes)
    exportar_fato_receita_doc_dw ()
    print("✔ Carga DW concluída.")

    print("Pipeline Receita por Tipo de Documento finalizado com sucesso.")


if __name__ == "__main__":
    pipeline_receita_doc()
