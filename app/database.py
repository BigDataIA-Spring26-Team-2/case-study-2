"""Database connection pooling."""
from contextlib import contextmanager
import snowflake.connector
from app.config import get_settings


class DatabaseManager:
    """Singleton database connection manager."""
    _instance = None
    _pool = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._pool is None:
            settings = get_settings()
            self._config = {
                'account': settings.snowflake.account,
                'user': settings.snowflake.user,
                'password': settings.snowflake.password.get_secret_value(),
                'warehouse': settings.snowflake.warehouse,
                'database': settings.snowflake.database,
                'schema': settings.snowflake.schema,
                'role': settings.snowflake.role
            }
    
    @contextmanager
    def get_connection(self):
        """Get a database connection from pool."""
        conn = snowflake.connector.connect(**self._config)
        try:
            yield conn
        finally:
            conn.close()


# Global instance
db_manager = DatabaseManager()


def get_db():
    """FastAPI dependency for database connection."""
    with db_manager.get_connection() as conn:
        yield conn