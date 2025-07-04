import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text

# Load environment variables
load_dotenv()


def test_database_connection():
    """Test database connection"""

    # Get connection data
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    database = os.getenv("DB_NAME", "egon-data")
    username = os.getenv("DB_USER", "egon-read")
    password = os.getenv("DB_PASSWORD", "")

    print(f"Try to connect to: {username}@{host}:{port}/{database}")

    # create connection
    connection_string = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"

    try:
        engine = create_engine(connection_string)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            print(f"✓ Connection successful! PostgreSQL Version: {version}")
            return True
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False


if __name__ == "__main__":
    test_database_connection()