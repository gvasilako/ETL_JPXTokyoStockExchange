from sqlalchemy import Column, Integer, DateTime, Float, Boolean, DECIMAL, ForeignKey
from sqlalchemy.orm import relationship
from .Base import Base


class StockPrices(Base):
    __tablename__ = 'StockPrices'

    id = Column(Integer, primary_key=True)
    Date = Column(DateTime, index=True)
    SecuritiesCode = Column(Integer, ForeignKey('StockMetadata.SecuritiesCode'), nullable=False, index=True)
    Open = Column(DECIMAL(16, 2))
    High = Column(DECIMAL(16, 2))
    Low = Column(DECIMAL(16, 2))
    Close = Column(DECIMAL(16, 2))
    Volume = Column(DECIMAL(65, 2))
    MarketCapitalization = Column(DECIMAL(65, 2))
    AdjustmentFactor = Column(Float)
    ExpectedDividend = Column(Float)
    SupervisionFlag = Column(Boolean)
    IsPrimary = Column(Boolean)
    CreatedDateTime = Column(DateTime)
    UpdatedDateTime = Column(DateTime)

    # Relationships
    stock = relationship('StockMetadata')