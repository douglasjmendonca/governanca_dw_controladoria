from prefect import flow
from pipeline_lancamentos.converter_parquet import converter_parquet
from pipeline_lancamentos.concaternar_parquet import concatenar_parquet
from pipeline_lancamentos.limpar_dados import limpar_dados
from pipeline_lancamentos.atribuir_regionais import atribuir_regionais
from pipeline_lancamentos.classificar_dre import classificar_dre
from pipeline_lancamentos.preparar_fato import preparar_fato
from pipeline_lancamentos.carregar_fato_dre import carregar_fato_dre


@flow(name="pipeline_controladoria")
def pipeline_controladoria():
    print("Iniciando pipeline da controladoria...")

    converter_parquet()
    concatenar_parquet()
    limpar_dados ()
    atribuir_regionais()
    classificar_dre()
    preparar_fato()
    carregar_fato_dre()

    print("Finalizando pipeline.")

if __name__ == "__main__":
    pipeline_controladoria()