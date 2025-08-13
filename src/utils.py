import os
import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine

def load_config(path="src/config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def get_engine(cfg):
    load_dotenv()
    url = os.getenv(cfg["db"]["url_env"])
    if not url:
        raise RuntimeError("DATABASE_URL is not set in environment or .env")
    return create_engine(url, future=True)
