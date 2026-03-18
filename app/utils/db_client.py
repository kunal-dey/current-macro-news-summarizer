"""SQLAlchemy session context manager."""

from typing import Optional

from sqlalchemy.orm import Session

from app.config.db import SessionLocal


class DBClient:
    def __init__(self):
        self.session: Optional[Session] = None

    def __enter__(self) -> "DBClient":
        self.session = SessionLocal()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session is None:
            return False
        try:
            if exc_type is None:
                self.session.commit()
            else:
                self.session.rollback()
        except Exception:
            self.session.rollback()
            raise
        finally:
            self.session.close()
            self.session = None
        return False
