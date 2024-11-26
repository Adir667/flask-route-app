from repositories.repo import ShipRouteRepository
from datetime import datetime
from sqlalchemy import func
from models.model import Ship, Message, WeatherData, WeatherStation
import requests
import folium
import os
from dotenv import load_dotenv

DEFAULT_DISTANCE_TOLERANCE = 1

class ShipRouteService:
    def __init__(self, session):
        self.session = session
        self.repo = ShipRouteRepository(session)

    def get_available_ships(self):
        ships = self.session.query(Ship).all()
        return [ship.name for ship in ships]
    
    def get_avg_speed_per_hour(self, date):
        # Convert the input date string to datetime object
        target_date = datetime.strptime(date, "%Y-%m-%d")
        
        # Convert the target datetime to string format 'YYYY-MM-DD' for SQLite
        target_date_str = target_date.strftime("%Y-%m-%d")

        # Query the database for messages on the target date, grouped by device_id and hour
        results = (self.session.query(
                        Message.ship_id.label('device_id'),
                        Ship.name.label('device_name'),
                        func.strftime('%H', Message.timestamp).label('hour'),
                        func.avg(Message.speed).label('avg_speed_knots')
                    )
                    .join(Ship, Message.ship_id == Ship.id)
                    .filter(func.date(Message.timestamp) == target_date_str)
                    .group_by(Message.ship_id, func.strftime('%H', Message.timestamp))
                    .all())

        result = {}

        for row in results:
            device_id = row.device_name
            hour = int(row.hour)
            avg_speed = round(row.avg_speed_knots, 2)

            if device_id not in result:
                result[device_id] = []

            result[device_id].append({
                'hour': hour,
                'avg_speed': avg_speed
            })

        return result

    def get_daily_wind_speed_per_ship(self, ship_name):
        try:
            wind_speed_results = {}

            # Step 1: Fetch messages for the given ship name and group them by day
            print(f"Fetching messages for ship: {ship_name}")
            ship_messages = (
                self.session.query(
                    Message.ship_id,
                    Message.timestamp,
                    Message.lat,
                    Message.lon,
                    func.date(Message.timestamp).label("date")
                )
                .join(Ship, Ship.id == Message.ship_id)
                .filter(Ship.name == ship_name)
                .all()
            )

            if not ship_messages:
                print(f"No messages found for ship with name: {ship_name}")
                return {}

            # Group messages by date
            ship_data = {}
            for message in ship_messages:
                date = message.date
                if date not in ship_data:
                    ship_data[date] = []
                ship_data[date].append({
                    "timestamp": message.timestamp,
                    "lat": message.lat,
                    "lon": message.lon,
                })
            print(f"Grouped messages by day: {list(ship_data.keys())}")

            # Step 2: Process each day's data
            for date, messages in ship_data.items():
                print(f"\nProcessing date: {date} with {len(messages)} messages")

                # Step 2a: Determine the time range (start_time and end_time) for the day
                start_time = min(msg["timestamp"].time() for msg in messages)
                end_time = max(msg["timestamp"].time() for msg in messages)
                print(f"Time range for {date}: {start_time} to {end_time}")

                # Step 2b: Find relevant stations for the day's messages
                relevant_stations = []
                for msg in messages:
                    lat, lon = msg["lat"], msg["lon"]
                    stations_nearby = (
                        self.session.query(WeatherStation)
                        .filter(
                            func.abs(WeatherStation.lat - lat) <= DEFAULT_DISTANCE_TOLERANCE,
                            func.abs(WeatherStation.lon - lon) <= DEFAULT_DISTANCE_TOLERANCE
                        )
                        .all()
                    )
                    for station in stations_nearby:
                        if station.station_id not in [s["station_id"] for s in relevant_stations]:
                            relevant_stations.append({
                                "station_id": station.station_id,
                                "lat": station.lat,
                                "lon": station.lon,
                            })

                print(f"Found {len(relevant_stations)} relevant stations for {date}")

                # Step 3: Query wind speed data for the relevant stations
                if relevant_stations:
                    wind_speed_data = (
                        self.session.query(
                            WeatherData.station_id,
                            func.max(WeatherData.wind_speed).label("max_wind_speed"),
                            func.min(WeatherData.wind_speed).label("min_wind_speed"),
                        )
                        .filter(
                            WeatherData.station_id.in_([s["station_id"] for s in relevant_stations]),
                            WeatherData.date == date,
                            WeatherData.time >= start_time,
                            WeatherData.time <= end_time,
                        )
                        .group_by(WeatherData.station_id)
                        .all()
                    )

                    if wind_speed_data:
                        print(f"Wind speed data for {date}: {wind_speed_data}")

                        # Extract max and min wind speed data
                        day_max_speed = max(wind_speed_data, key=lambda x: x.max_wind_speed)
                        day_min_speed = min(wind_speed_data, key=lambda x: x.min_wind_speed)

                        wind_speed_results[date] = {
                            "max_wind_speed": day_max_speed.max_wind_speed,
                            "min_wind_speed": day_min_speed.min_wind_speed,
                        }

                    else:
                        print(f"No wind speed data found for {date}")
                else:
                    print(f"No relevant stations found for {date}")

            return wind_speed_results

        except Exception as e:
            print(f"Error in get_daily_wind_speed_per_ship: {str(e)}")
            return {"error": str(e)}

    def get_daily_weather_data_per_ship(self, ship_name, date):
        try:
            print(f"Fetching first valid message for ship: {ship_name} on date: {date}")
            
            # Step 1: Fetch the first valid message for the given ship name and date
            ship_message = (
                self.session.query(
                    Message.ship_id,
                    Message.timestamp,
                    Message.lat,
                    Message.lon,
                    func.date(Message.timestamp).label("date")
                )
                .join(Ship, Ship.id == Message.ship_id)
                .filter(Ship.name == ship_name, func.date(Message.timestamp) == date)
                .order_by(Message.timestamp)
                .first()
            )

            if not ship_message:
                print(f"No valid messages found for ship '{ship_name}' on date '{date}'.")
                return {}

            # Step 2: Get the location (latitude, longitude) and timestamp from the first message
            lat, lon = ship_message.lat, ship_message.lon
            timestamp = ship_message.timestamp
            print(f"First message for {ship_name} on {date} at {lat}, {lon} with timestamp {timestamp}")

            # Step 3: Query all stations (instead of using a tolerance filter)
            stations = self.session.query(WeatherStation).all()

            # Step 4: Find the closest station by calculating absolute differences in lat/lon
            closest_station = None
            smallest_diff = float('inf')  # Initialize with a large number

            for station in stations:
                # Calculate the absolute differences in lat/lon
                lat_diff = abs(station.lat - lat)
                lon_diff = abs(station.lon - lon)
                total_diff = lat_diff + lon_diff  # Combine the differences

                # If this station is closer, update the closest station
                if total_diff < smallest_diff:
                    smallest_diff = total_diff
                    closest_station = station

            if not closest_station:
                print(f"No closest station found.")
                return {}

            print(f"Closest station found: {closest_station.station_id} at ({closest_station.lat}, {closest_station.lon})")

            # Step 5: Query all available weather data for the closest station on the given date
            weather_data = (
                self.session.query(
                    WeatherData.station_id,
                    WeatherData.temperature,
                    WeatherData.wind_speed,
                    WeatherData.description,
                    WeatherData.time
                )
                .filter(
                    WeatherData.station_id == closest_station.station_id,
                    WeatherData.date == date
                )
                .all()
            )

            if not weather_data:
                print(f"No weather data found for closest station on {date}")
                return {}

            # Step 6: Find the closest weather report timestamp to the ship's timestamp
            closest_time_diff = float('inf')  # Initialize with a large number
            closest_weather_report = None

            for data in weather_data:
                weather_time = data.time

                # Convert both times to datetime objects for comparison (set arbitrary date)
                ship_time = datetime.combine(datetime.today(), timestamp.time())
                weather_data_time = datetime.combine(datetime.today(), weather_time)

                # Calculate the time difference in seconds
                time_diff = abs((weather_data_time - ship_time).total_seconds())

                # If this weather report is closer, update
                if time_diff < closest_time_diff:
                    closest_time_diff = time_diff
                    closest_weather_report = data

            if not closest_weather_report:
                print(f"No weather report found for closest time.")
                return {}

            # Step 7: Get an image by weather description
            base_query = 'cargo ship in'
            full_query = f"{base_query} {closest_weather_report.description}"
            image_url = self.get_image_from_unsplash(full_query)

            # Step 8: Get an image by weather description
            route_html = self.generate_visual_route_on_map(ship_name, date)

            # Step 9: Return the weather data in a simple format
            return {
                "temperature": closest_weather_report.temperature,
                "wind_speed": closest_weather_report.wind_speed,
                "description": closest_weather_report.description,
                "image_url": image_url,
                "route_html": route_html
            }

        except Exception as e:
            print(f"Error in get_daily_weather_data_per_ship: {str(e)}")
            return {"error": str(e)}

    def get_image_from_unsplash(self, query):
        try:
            # Access the Unsplash API key from environment variables
            access_key = os.getenv("UNSPLASH_ACCESS_KEY")
            if not access_key:
                raise ValueError("Unsplash API access key not set in environment variables.")

            # Base URL for Unsplash search API
            url = f"https://api.unsplash.com/search/photos?query={query}&client_id={access_key}"

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            if data['results']:
                return str(data['results'][0]['urls']['regular']).split('?')[0]
            else:
                return None
        except Exception as e:
            print(f"Error fetching image from Unsplash: {e}")
            return None
        
    def generate_visual_route_on_map(self, ship_name, day):
        # Query the ship by name to get its ID
        ship = self.session.query(Ship).filter(Ship.name == ship_name).first()
        if not ship:
            print(f"Ship with name '{ship_name}' not found.")
            return

        # Query relevant messages using the ship ID
        messages = self.session.query(Message).filter(
            Message.ship_id == ship.id,
            Message.timestamp >= f"{day} 00:00:00",
            Message.timestamp <= f"{day} 23:59:59"
        ).order_by(Message.timestamp).all()

        # Ensure we have data
        if not messages:
            print(f"No data found for Ship: {ship_name} on Date: {day}")
            return "no available visual route"

        # Extract coordinates
        coordinates = [(msg.lat, msg.lon) for msg in messages]

        # Initialize the map centered at the first coordinate
        m = folium.Map(location=coordinates[0], zoom_start=10, tiles="OpenStreetMap")
        
        # Add route line
        folium.PolyLine(coordinates, color="blue", weight=2.5, opacity=1).add_to(m)
        
        # Add markers for each point
        for i, coord in enumerate(coordinates):
            folium.Marker(coord, tooltip=f"Point {i + 1}").add_to(m)

        # Prepare paths
        maps_dir = "data/maps"  # Base directory for maps
        ship_dir = os.path.join(maps_dir, ship_name)  # Subdirectory for the ship
        day_file = os.path.join(ship_dir, f"{day}.html")  # Full path for the HTML file

        # Ensure directories exist
        os.makedirs(ship_dir, exist_ok=True)

        # Save the map
        m.save(day_file)
        print(f"Map saved to {day_file}")
        return f"maps/{ship_name}/{day}.html"