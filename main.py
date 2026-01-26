from etl.transform import transform
from etl.load import load_postgres


def run_pipeline():
    print("Iniciando pipeline ETL")

    # Transform - transforma os dados
    transform()

    # Load - carrega os dados no postgreSQL
    load_postgres()

    print("Pipeline finalizado com sucesso")


if __name__ == "__main__":
    run_pipeline()
