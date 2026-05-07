
import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

# ───────────────────────────────
# SOURCE (BRONZE)
# ───────────────────────────────
SRC_HOST = os.getenv("MYSQL_HOST_SOURCE")
SRC_PORT = int(os.getenv("MYSQL_PORT_SOURCE", 3306))
SRC_USER = os.getenv("MYSQL_USER_SOURCE")
SRC_PASSWORD = os.getenv("MYSQL_PASSWORD_SOURCE")
BRONZE_DB = os.getenv("MYSQL_BRONZE_DB")

# ───────────────────────────────
# DESTINATION (GOLD)
# ───────────────────────────────
DEST_HOST = os.getenv("MYSQL_HOST_DESTINATION")
DEST_PORT = int(os.getenv("MYSQL_PORT_DESTINATION", 3306))
DEST_USER = os.getenv("MYSQL_USER_DESTINATION")
DEST_PASSWORD = os.getenv("MYSQL_PASSWORD_DESTINATION")
GOLD_DB = os.getenv("MYSQL_GOLD_DB")


# ───────────────────────────────
# CONNECTIONS
# ───────────────────────────────

def get_bronze_connection():
    return mysql.connector.connect(
        host=SRC_HOST,
        port=SRC_PORT,
        user=SRC_USER,
        password=SRC_PASSWORD,
        database=BRONZE_DB,
        autocommit=False
    )


def get_gold_connection():
    return mysql.connector.connect(
        host=DEST_HOST,
        port=DEST_PORT,
        user=DEST_USER,
        password=DEST_PASSWORD,
        database=GOLD_DB,
        autocommit=False
    )