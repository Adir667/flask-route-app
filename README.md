This application provides endpoints for ship-related data and weather conditions. It is containerized for easy deployment and interaction.

The application is containerized and can be publicly pulled from:
https://hub.docker.com/r/adir667/flask-route-app
docker pull adir667/flask-route-app

Also, hosted:
https://flask-route-app.onrender.com/

Attached to this project is postman collection for interacting with the enpoints:

1.[GET] /available_ships
Returns a list of available ships.

2.[GET] /avg_speed_per_hour?date=
Provides the average speed per hour for all ships on the specified date (currently fixed to 2019-02-13).

3.[GET] /wind_speed_daily/<ship_id>
Retrieves daily wind speed data for the specified ship ID. (currently fixed to st-1a2090)

4.[GET] /visualize_conditions/<ship_id>?date=
Returns a visualization (summary) of weather conditions for the specified ship and date (currently fixed to st-1a2090 and 2019-02-13).

5.[GET] /download_db
Allows downloading the SQLite database used by the application.

