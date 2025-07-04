import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from sshtunnel import SSHTunnelForwarder

load_dotenv()


class ForeignKeyValidationCheck:
    """Validates foreign key relationships between tables"""

    def __init__(self):
        self.name = "foreign_key_integrity_check"

    def validate_with_ssh(self):
        """Validates that all loads are connected to valid buses"""

        # SSH and DB configuration (same as before)
        ssh_config = {
            'host': os.getenv("SSH_HOST"),
            'user': os.getenv("SSH_USER"),
            'key': os.path.expanduser(os.getenv("SSH_KEY_FILE")),
            'local_port': int(os.getenv("SSH_LOCAL_PORT")),
            'remote_port': int(os.getenv("SSH_REMOTE_PORT"))
        }

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

                connection_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['name']}"
                engine = create_engine(connection_string)

                # Validation: Check if all loads reference valid buses
                query = """
                SELECT 
                    COUNT(*) as total_loads,
                    COUNT(CASE WHEN b.bus_id IS NOT NULL THEN 1 END) as loads_with_valid_bus,
                    COUNT(CASE WHEN b.bus_id IS NULL THEN 1 END) as orphaned_loads
                FROM grid.egon_etrago_load l
                LEFT JOIN grid.egon_etrago_bus b 
                    ON l.bus = b.bus_id 
                    AND l.scn_name = b.scn_name
                WHERE l.scn_name = 'eGon2035'
                LIMIT 1000
                """

                result = pd.read_sql(query, engine)

                total = result.iloc[0]['total_loads']
                valid = result.iloc[0]['loads_with_valid_bus']
                orphaned = result.iloc[0]['orphaned_loads']

                print(f"📊 Results:")
                print(f"   Total loads: {total}")
                print(f"   Loads with valid bus: {valid}")
                print(f"   Orphaned loads: {orphaned}")

                if orphaned > 0:
                    print(f"❌ CRITICAL ERROR: {orphaned} loads are not connected to valid buses!")
                    return False
                else:
                    print(f"✅ SUCCESS: All loads are connected to valid buses!")
                    return True

        except Exception as e:
            print(f"❌ Validation failed: {e}")
            return False


if __name__ == "__main__":
    validator = ForeignKeyValidationCheck()
    success = validator.validate_with_ssh()
    print(f"\n🎯 Validation {'PASSED' if success else 'FAILED'}")