import os
from dotenv import load_dotenv

load_dotenv()  # Carrega o .env da raiz

def get_db_config(db: str = "gcp") -> dict:
    if db == "gcp":
        return {
            "host": os.getenv("GCP_HOST"),
            "database": os.getenv("GCP_DATABASE"),
            "user": os.getenv("GCP_USER"),
            "password": os.getenv("GCP_PASSWORD"),
        }
    elif db == "imanager":
        return {
            "host": os.getenv("IM_HOST"),
            "database": os.getenv("IM_DATABASE"),
            "user": os.getenv("IM_USER"),
            "password": os.getenv("IM_PASSWORD"),
        }
    elif db == "local":
        return {
            "host": os.getenv("LOCAL_HOST"),
            "database": os.getenv("LOCAL_DATABASE"),
            "user": os.getenv("LOCAL_USER"),
            "password": os.getenv("LOCAL_PASSWORD"),
        }
    elif db == "dw":
        # Usa o schema na conexão via options -> search_path
        schema = os.getenv("PG_SCHEMA", "public")
        return {
            "host": os.getenv("PG_HOST"),
            "database": os.getenv("PG_DB"),
            "user": os.getenv("PG_USER"),
            "password": os.getenv("PG_PASSWORD"),
            "port": int(os.getenv("PG_PORT", "5432")),
            "options": f"-c search_path={schema}",
        }
    else:
        raise ValueError(f"Banco '{db}' não reconhecido.")
