import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "superfresh")

cliente = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)

def obtener_bd():
    cliente.admin.command("ping")
    return cliente[MONGO_DB]

def mongo_disponible():
    try:
        cliente.admin.command("ping")
        return True
    except ServerSelectionTimeoutError:
        return False
