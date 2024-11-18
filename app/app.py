from flask import Flask, request, jsonify, send_file
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from services.service import ShipRouteService
from models.model import Base

app = Flask(__name__)

# Database connection setup
DATABASE_URL = "sqlite:///data/database.db"  # Adjust this for your database type
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

# Create the tables in the database if they don't exist
Base.metadata.create_all(engine)  # This will create all the tables defined in your models

service = ShipRouteService(session)

@app.route('/')
def home():
    return 'hello flask!'

@app.route('/available_ships', methods=['GET'])
def get_available_ships():
    try:
        available_ships = service.get_available_ships()
        
        return jsonify({
            "total_ships": len(available_ships),
            "ship_names": available_ships
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/avg_speed_per_hour', methods=['GET'])
def get_avg_speed_per_hour():
    date = request.args.get('date')

    # Hardcoded assignment requirement
    if date != "2019-02-13":
        return jsonify({"error": "Only data for the date 2019-02-13 is accepted."}), 400

    try:
        avg_speed_by_hour = service.get_avg_speed_per_hour(date)
        return avg_speed_by_hour

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/wind_speed_daily/<ship>', methods=['GET'])
def get_wind_speed(ship):

    # Hardcoded assignment requirement
    if ship != "st-1a2090":
        return jsonify({"error": "Only data for ship st-1a2090 is accepted."}), 400
    
    try:
        wind_data = service.get_daily_wind_speed_per_ship(ship)
        return wind_data
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/visualize_conditions/<ship>', methods=['GET'])
def get_visual_conditions(ship):
    date = request.args.get('date')

    # Hardcoded assignment requirement
    if ship != "st-1a2090" or date != "2019-02-13":
        return jsonify({"error": "Condition visualization is accepted only data for ship st-1a2090 at date 2019-02-13."}), 400

    try:
        conditions = service.get_daily_weather_data_per_ship(ship, date)
        if not conditions:
            return jsonify({"error": "No valid data found"}), 404
        return conditions
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/download_db', methods=['GET'])
def download_db():
    try:
        db_file_path = 'data/database.db'
        return send_file(db_file_path, as_attachment=True, download_name='database.db', mimetype='application/x-sqlite3')
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
