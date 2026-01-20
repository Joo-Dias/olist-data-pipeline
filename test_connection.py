from database.connection import get_engine

engine = get_engine()

with engine.connect() as conn:
    print("Conexão com PostgreSQL realizada com sucesso!")