import os
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.schema import Base


@pytest.fixture(scope="session")
def mock_edgar():
    os.environ["MOCK_EDGAR"] = "true"
    yield
    if "MOCK_EDGAR" in os.environ:
        del os.environ["MOCK_EDGAR"]


@pytest.fixture
def db_engine():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)


@pytest.fixture
def db_session(db_engine):
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=db_engine)
    session = SessionLocal()
    yield session
    session.close()


@pytest.fixture
def sample_ixbrl_html():
    return """
    <html>
    <body>
        <span contextRef="FY2023" unitRef="USD" decimals="0" name="us-gaap:Revenues">123456789</span>
        <span contextRef="FY2023" unitRef="USD" decimals="0" name="us-gaap:NetIncomeLoss">12345678</span>
        <span contextRef="FY2023" unitRef="USD" decimals="0" name="us-gaap:Assets">98765432</span>
    </body>
    </html>
    """
