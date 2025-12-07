import time
import schedule
import mysql.connector
import os
import logging
import requests
import json
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database configuration from Environment Variables
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = os.getenv('DB_USER', 'aqi_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'aqi_password')
DB_NAME = os.getenv('DB_NAME', 'aqi_telemetry')

# Airthings Configuration
CLIENT_ID = os.getenv('AIRTHINGS_CLIENT_ID')
CLIENT_SECRET = os.getenv('AIRTHINGS_CLIENT_SECRET')
BASE_URL = "https://ext-api.airthings.com/v1"
TOKEN_URL = "https://accounts-api.airthings.com/v1/token"

# Weather Configuration
WEATHER_ZIP_CODE = os.getenv('WEATHER_ZIP_CODE')
WEATHER_USER_AGENT = "AQI-gator/1.0 (your_email@example.com)"

class WeatherClient:
    def __init__(self, zip_code):
        self.zip_code = zip_code
        self.station_id = None
        self.headers = {"User-Agent": WEATHER_USER_AGENT}

    def _resolve_station(self):
        """Resolves Zip Code to NWS Station ID."""
        if self.station_id:
            return self.station_id

        logging.info(f"Resolving station for Zip Code: {self.zip_code}")
        try:
            # Step 1: Zip -> Lat/Lon
            geo_url = f"http://api.zippopotam.us/us/{self.zip_code}"
            geo_resp = requests.get(geo_url, timeout=10)
            geo_resp.raise_for_status()
            geo_data = geo_resp.json()
            
            if not geo_data.get('places'):
                logging.error("Invalid Zip Code")
                return None
                
            lat = geo_data['places'][0]['latitude']
            lon = geo_data['places'][0]['longitude']
            
            # Step 2: Lat/Lon -> Gridpoint -> Stations
            points_url = f"https://api.weather.gov/points/{lat},{lon}"
            points_resp = requests.get(points_url, headers=self.headers, timeout=10)
            points_resp.raise_for_status()
            points_data = points_resp.json()
            
            stations_url = points_data['properties']['observationStations']
            
            # Step 3: Get first station
            stations_resp = requests.get(stations_url, headers=self.headers, timeout=10)
            stations_resp.raise_for_status()
            stations_data = stations_resp.json()
            
            if not stations_data.get('features'):
                logging.error("No weather stations found for this location")
                return None
                
            # Get the station identifier (e.g., "KNYC")
            self.station_id = stations_data['features'][0]['properties']['stationIdentifier']
            logging.info(f"Resolved weather station: {self.station_id}")
            return self.station_id

        except Exception as e:
            logging.error(f"Failed to resolve weather station: {e}")
            return None

    def get_current_conditions(self):
        """Fetches latest observations from NWS."""
        station_id = self._resolve_station()
        if not station_id:
            return None

        url = f"https://api.weather.gov/stations/{station_id}/observations/latest"
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            props = data.get('properties', {})
            
            # Extract relevant metrics
            metrics = {}
            
            if props.get('temperature', {}).get('value') is not None:
                metrics['temp'] = props['temperature']['value'] # Celsius
            
            if props.get('relativeHumidity', {}).get('value') is not None:
                metrics['humidity'] = props['relativeHumidity']['value'] # %
                
            if props.get('barometricPressure', {}).get('value') is not None:
                # Convert Pa to hPa (mbar)
                metrics['pressure'] = props['barometricPressure']['value'] / 100.0
                
            return metrics

        except Exception as e:
            logging.error(f"Failed to fetch weather data: {e}")
            return None

class AirthingsClient:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.token_expiry = 0

    def _get_token(self):
        """Retrieves a new access token using Client Credentials flow."""
        logging.info("Refreshing access token...")
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        try:
            response = requests.post(TOKEN_URL, data=payload, timeout=10)
            response.raise_for_status()
            data = response.json()
            self.access_token = data['access_token']
            self.token_expiry = time.time() + data.get('expires_in', 3600) - 60
            logging.info("Token refreshed successfully.")
        except Exception as e:
            logging.error(f"Failed to get token: {e}")
            raise

    def _ensure_token(self):
        """Ensures a valid token exists."""
        if not self.access_token or time.time() >= self.token_expiry:
            self._get_token()

    def _request(self, method, endpoint, **kwargs):
        """Wrapper for requests with auto-token refresh."""
        self._ensure_token()
        headers = kwargs.get('headers', {})
        headers['Authorization'] = f"Bearer {self.access_token}"
        kwargs['headers'] = headers
        
        url = f"{BASE_URL}{endpoint}"
        try:
            response = requests.request(method, url, **kwargs)
            if response.status_code == 401:
                logging.warning("Token expired during request, refreshing and retrying...")
                self._get_token()
                headers['Authorization'] = f"Bearer {self.access_token}"
                response = requests.request(method, url, **kwargs)
            
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"Request to {endpoint} failed: {e}")
            return None

    def get_locations(self):
        """Fetches all locations."""
        data = self._request("GET", "/locations")
        return data.get('locations', []) if data else []

    def get_latest_samples(self, location_id):
        """Fetches latest samples for a specific location."""
        return self._request("GET", f"/locations/{location_id}/latest-samples")

def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        return conn
    except mysql.connector.Error as err:
        logging.error(f"Error connecting to database: {err}")
        return None

def fetch_and_store_data():
    """
    Fetches data from Airthings API and stores it in MySQL.
    """
    logging.info("Job started: Fetching telemetry data...")
    
    if not CLIENT_ID or not CLIENT_SECRET:
        logging.error("AIRTHINGS_CLIENT_ID or AIRTHINGS_CLIENT_SECRET not set.")
        return

    client = AirthingsClient(CLIENT_ID, CLIENT_SECRET)
    
    try:
        locations = client.get_locations()
        logging.info(f"Found {len(locations)} locations.")
        
        conn = get_db_connection()
        if not conn:
            logging.warning("Skipping data insertion due to DB connection failure.")
            return

        cursor = conn.cursor()
        query = "INSERT INTO telemetry (source, metric, value, timestamp) VALUES (%s, %s, %s, %s)"
        current_time = datetime.now()
        
        records_inserted = 0
        
        for loc in locations:
            loc_id = loc['id']
            loc_name = loc.get('name', 'Unknown')
            logging.info(f"Fetching samples for location: {loc_name} ({loc_id})")
            
            samples_data = client.get_latest_samples(loc_id)
            
            if samples_data and 'devices' in samples_data:
                for device_sample in samples_data['devices']:
                    device_id = device_sample.get('id')
                    data = device_sample.get('data', {})
                    
                    # Iterate over all metrics in the data dictionary
                    for metric, value in data.items():
                        # Skip non-numeric values if necessary, or handle them
                        if isinstance(value, (int, float)):
                            try:
                                cursor.execute(query, (f"{loc_name}_{device_id}", metric, value, current_time))
                                records_inserted += 1
                            except mysql.connector.Error as err:
                                logging.error(f"Error inserting record: {err}")
            else:
                logging.warning(f"No sample data found for location {loc_name}")

        conn.commit()
        logging.info(f"Successfully inserted {records_inserted} records.")
        cursor.close()
        conn.close()

    except Exception as e:
        logging.error(f"Job failed: {e}")

    # --- Weather Data Collection ---
    if WEATHER_ZIP_CODE:
        logging.info("Fetching weather data...")
        weather_client = WeatherClient(WEATHER_ZIP_CODE)
        weather_data = weather_client.get_current_conditions()
        
        if weather_data:
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                query = "INSERT INTO telemetry (source, metric, value, timestamp) VALUES (%s, %s, %s, %s)"
                current_time = datetime.now()
                source_name = f"weather_{WEATHER_ZIP_CODE}"
                
                try:
                    for metric, value in weather_data.items():
                        cursor.execute(query, (source_name, metric, value, current_time))
                    conn.commit()
                    logging.info(f"Successfully inserted weather data for {source_name}")
                except mysql.connector.Error as err:
                    logging.error(f"Error inserting weather data: {err}")
                finally:
                    cursor.close()
                    conn.close()
        else:
            logging.warning("No weather data retrieved.")

def main():
    logging.info("Worker started. Waiting for database...")
    
    # Simple wait-for-it logic
    while True:
        conn = get_db_connection()
        if conn:
            conn.close()
            logging.info("Database connection successful!")
            break
        logging.info("Database not ready yet, retrying in 5 seconds...")
        time.sleep(5)

    # Schedule the job every 5 minutes
    schedule.every(5).minutes.do(fetch_and_store_data)

    logging.info("Scheduler running. Press Ctrl+C to exit.")
    
    # Run the first job immediately
    fetch_and_store_data()

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
