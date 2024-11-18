from sqlalchemy import Column, Integer, String, Float, DateTime, Date, Time, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.types import Date

Base = declarative_base()

class Ship(Base):
    __tablename__ = 'ships'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    messages = relationship("Message", back_populates="ship")


class Message(Base):
    __tablename__ = 'messages'
    id = Column(Integer, primary_key=True, autoincrement=True)
    ship_id = Column(Integer, ForeignKey('ships.id'))
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    speed = Column(Float, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    ship = relationship("Ship", back_populates="messages")


class WeatherStation(Base):
    __tablename__ = 'weather_stations'
    station_id = Column(String, primary_key=True)
    station_name = Column(String, nullable=False)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)


class WeatherData(Base):
    __tablename__ = 'weather_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(String, ForeignKey('weather_stations.station_id'), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    date = Column(Date, nullable=False)
    time = Column(Time, nullable=False)
    temperature = Column(Float, nullable=True)
    wind_speed = Column(Float, nullable=True)
    description = Column(String, nullable=True)
    station = relationship("WeatherStation")
