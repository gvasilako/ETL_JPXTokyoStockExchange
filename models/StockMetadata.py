from sqlalchemy import Column, Integer, String, DateTime, Text
from .Base import Base


class StockMetadata(Base):
    __tablename__ = 'StockMetadata'

    SecuritiesCode = Column(Integer, primary_key=True, autoincrement=False)
    Name = Column(Text)
    Section = Column(String(512))
    NewMarketSegment = Column(String(512))
    SectorName33 = Column(String(512))
    SectorName17 = Column(String(512))
    NewIndexSeriesSize = Column(String(512))
    CreatedDateTime = Column(DateTime)
    UpdatedDateTime = Column(DateTime)
