import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from sshtunnel import SSHTunnelForwarder

load_dotenv()


class SimpleTimeSeriesCheck:
    """First simple validation rule for time series"""

    def __init__(self):
        self.name = "time_series_length_check"

    def validate_with_ssh(self):
        """Validates time series lengths via SSH tunnel"""

        # SSH configuration
        ssh_config = {
            'host': os.getenv("SSH_HOST"),
            'user': os.getenv("SSH_USER"),
            'key': os.path.expanduser(os.getenv("SSH_KEY_FILE")),
            'local_port': int(os.getenv("SSH_LOCAL_PORT")),
            'remote_port': int(os.getenv("SSH_REMOTE_PORT"))
        }

        # Database configuration
        db_config = {
            'host': os.getenv("DB_HOST"),
            'port': int(os.getenv("DB_PORT")),
            'name': os.getenv("DB_NAME"),
            'user': os.getenv("DB_USER"),
            'password': os.getenv("DB_PASSWORD")
        }

        print(f"🔍 Validation: {self.name}")

        try:
            with SSHTunnelForwarder(
                    (ssh_config['host'], 22),
                    ssh_username=ssh_config['user'],
                    ssh_pkey=ssh_config['key'],
                    remote_bind_address=('localhost', ssh_config['remote_port']),
                    local_bind_address=('localhost', ssh_config['local_port'])
            ) as tunnel:

                print("✓ SSH tunnel started")

                # Database connection
                connection_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['name']}"

                engine = create_engine(connection_string)

                # First real validation: Check time series lengths
                query = """
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(CASE WHEN cardinality(p_set) = 8760 THEN 1 END) as correct_length,
                    COUNT(CASE WHEN cardinality(p_set) != 8760 THEN 1 END) as wrong_length
                FROM grid.egon_etrago_load_timeseries 
                WHERE scn_name = 'eGon2035'
                LIMIT 1000
                """

                result = pd.read_sql(query, engine)

                total = result.iloc[0]['total_rows']
                correct = result.iloc[0]['correct_length']
                wrong = result.iloc[0]['wrong_length']

                print(f"📊 Results:")
                print(f"   Total rows: {total}")
                print(f"   Correct length (8760): {correct}")
                print(f"   Wrong length: {wrong}")

                if wrong > 0:
                    print(f"❌ CRITICAL ERROR: {wrong} time series have wrong length!")
                    return False
                else:
                    print(f"✅ SUCCESS: All time series have correct length!")
                    return True

        except Exception as e:
            print(f"❌ Validation failed: {e}")
            return False


if __name__ == "__main__":
    validator = SimpleTimeSeriesCheck()
    success = validator.validate_with_ssh()
    print(f"\n🎯 Validation {'PASSED' if success else 'FAILED'}")