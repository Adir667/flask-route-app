import pandas as pd
import json
import re
from datetime import datetime

from models.model import Ship, Message, WeatherStation, WeatherData

# Initialize the service with a default file
DEFAULT_MESSAGES_FILE_PATH = 'data/cleaned_messages_data.csv'
DEFAULT_WEATHER_FILE_PATH = 'data/weather_data.json'

class ShipRouteRepository:
    def __init__(self, session):
        self.messages_data = self._load_cleaned_messages_data()
        self.weather_data = self._load_weather_data()
        self.session = session  # Attach the session
        self._persist_data()

    def _load_cleaned_messages_data(self):
        df = pd.read_csv(DEFAULT_MESSAGES_FILE_PATH)
        return df
     
    def _load_weather_data(self):
        with open(DEFAULT_WEATHER_FILE_PATH, 'r') as file:
            data = json.load(file)
            return data

    def _persist_data(self):
        try:
            # Step 1: Process Messages and Ships Data
            ships = self.messages_data['device_id'].unique().tolist()  # Unique list of ship identifiers

            # Create or check for existing Ship entries
            for s in ships:
                # Check if the ship already exists to avoid duplicates
                existing_ship = self.session.query(Ship).filter_by(name=s).first()

                if not existing_ship:
                    # If the ship doesn't exist, create and add it
                    ship = Ship(name=s)
                    self.session.add(ship)
                    self.session.commit()  # Commit to get the ship's ID
                else:
                    ship = existing_ship

                # Insert Message entries associated with the ship
                for idx, row in self.messages_data[self.messages_data['device_id'] == s].iterrows():
                    # Parse timestamp and extract readable_date
                    timestamp = datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S')

                    message = Message(
                        ship_id=ship.id,
                        lat=row['lat'],
                        lon=row['lon'],
                        timestamp=timestamp,
                        speed=row['speed (knots)']
                    )

                    self.session.add(message) 

            # Step 2: Process Weather Data and Weather Stations
            for item in self.weather_data:
                # Check if the station already exists in the database
                existing_station = self.session.query(WeatherStation).filter_by(station_id=item['station_id']).first()

                if not existing_station:
                    # If the station doesn't exist, create and add it
                    weather_station = WeatherStation(
                        station_id=item['station_id'],
                        station_name=item['city_name'],
                        lat=item['lat'],
                        lon=item['lon']
                    )
                    self.session.add(weather_station)

                # Loop over hourly weather data and add to WeatherData
                    for i in range(24):
                        # Convert the timestamp string to a datetime object
                        timestamp_str = item['data'][i]['timestamp_utc']
                        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S')
                        
                        # The following breakdown helps with filtering later on
                        date = timestamp.date()
                        time = timestamp.time()

                        # Create WeatherData entry
                        weather_data_entry = WeatherData(
                            station_id=item['station_id'],
                            timestamp=timestamp,
                            temperature=item['data'][i]['temp'],
                            wind_speed=item['data'][i]['wind_spd'],
                            description=item['data'][i]['weather']['description'],
                            date=date,
                            time=time
                        )
                    self.session.add(weather_data_entry)  # Add to session

            # Step 3: Commit all changes at once
            self.session.commit()

        except Exception as e:
            self.session.rollback()
            print(f"Failed to persist data: {e}")

        finally:
            self.session.close()