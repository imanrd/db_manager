import sqlite3
import traceback
import logging
from typing import Optional, Any, Tuple

logger = logging.getLogger(__name__)


class DBConnector:
    def __init__(self, db_name: str):
        self.db_name = db_name
        self.conn = None

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()

    def execute_query(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> Optional[sqlite3.Cursor]:
        if not self.conn:
            logger.info('DBConnector: Connection not established')
            return None
        try:
            cursor = self.conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            self.conn.commit()
            return cursor
        except sqlite3.Error as e:
            logger.error(f"sqlite Error {e}: {traceback.format_exc()}")
            return None
        except Exception as e:
            logger.error(f"Exception {e}: {traceback.format_exc()}")
