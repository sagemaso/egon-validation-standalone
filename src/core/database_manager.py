import os
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from dotenv import load_dotenv

load_dotenv()


class DatabaseManager:
    """Centralized database connection management"""

    def __init__(self, use_ssh_tunnel: bool = True):
        self.use_ssh_tunnel = use_ssh_tunnel
        self.engine = None
        self.tunnel = None

    @contextmanager
    def connection_context(self):
        """Provides database connection context with automatic cleanup"""

        if self.use_ssh_tunnel:
            # Setup SSH tunnel
            ssh_config = {
                'host': os.getenv("SSH_HOST"),
                'user': os.getenv("SSH_USER"),
                'key': os.path.expanduser(os.getenv("SSH_KEY_FILE")),
                'local_port': int(os.getenv("SSH_LOCAL_PORT")),
                'remote_port': int(os.getenv("SSH_REMOTE_PORT"))
            }

            tunnel = SSHTunnelForwarder(
                (ssh_config['host'], 22),
                ssh_username=ssh_config['user'],
                ssh_pkey=ssh_config['key'],
                remote_bind_address=('localhost', ssh_config['remote_port']),
                local_bind_address=('localhost', ssh_config['local_port'])
            )

            try:
                tunnel.start()
                engine = self._create_engine()
                yield engine
            finally:
                if tunnel:
                    tunnel.stop()
        else:
            # Direct connection (for pipeline integration later)
            engine = self._create_engine()
            yield engine

    def _create_engine(self):
        """Creates SQLAlchemy engine"""
        db_config = {
            'host': os.getenv("DB_HOST"),
            'port': int(os.getenv("DB_PORT")),
            'name': os.getenv("DB_NAME"),
            'user': os.getenv("DB_USER"),
            'password': os.getenv("DB_PASSWORD")
        }

        connection_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['name']}"
        return create_engine(connection_string)

    def execute_query(self, query: str, engine=None) -> pd.DataFrame:
        """Execute SQL query and return DataFrame"""
        if engine is None:
            with self.connection_context() as engine:
                return pd.read_sql(query, engine)
        else:
            return pd.read_sql(query, engine)