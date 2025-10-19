from sqlalchemy import create_engine, String, Integer, DateTime, Text, UniqueConstraint, Float
from sqlalchemy.orm import DeclarativeBase, mapped_column, sessionmaker, Mapped
from datetime import datetime
from .settings import settings

# Database connection and session setup
engine = create_engine(settings.DATABASE_URL, echo=False)
SessionLocal = sessionmaker(expire_on_commit = False,  bind=engine)

# Base class for models

class Base(DeclarativeBase):
    pass

# Article model definition
class Article(Base):
    __tablename__ = "articles"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(128))  # News source
    url: Mapped[str] = mapped_column(String(1024))  # URL of the article
    title: Mapped[str] = mapped_column(String(1024)) # Title of the article
    published_at: Mapped[datetime] = mapped_column(DateTime)  # Publication date
    author: Mapped[str] = mapped_column(String(256), default = "") # Author, if known
    summary: Mapped[str] = mapped_column(Text, default = "")  # Summary
    text: Mapped[str] = mapped_column(Text, default = "")     # Full text, if available
    tickers: Mapped[str] = mapped_column(String(512), default = "") # Comma-separated list of tickers

    __table_args__ = (UniqueConstraint("url", name = "uq_article_url"),)

# New table with stock prices
class Price(Base):
    __tablename__ = "prices"
    id: Mapped[int] = mapped_column(Integer, primary_key = True)
    ticker: Mapped[str] = mapped_column(String(16))
    date: Mapped[datetime] = mapped_column(DateTime)
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    adj_close: Mapped[float] = mapped_column(Float)
    volume: Mapped[int] = mapped_column(Integer)
    close: Mapped[float] = mapped_column(Float)
    __table_args__ = (UniqueConstraint("ticker", "date", name =  "uq_price_ticker_date"),)


# Create tables
if __name__ == "__main__":
    Base.metadata.create_all(engine)
    print("Tables created in", settings.DATABASE_URL)

