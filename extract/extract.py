import os
import time
import requests
import logging
import json
import sys
from datetime import datetime, timedelta, timezone

from google.cloud import pubsub_v1
from dotenv import load_dotenv

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

load_dotenv()

TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
PUB_SUB_TOPIC = os.getenv("PUB_SUB_TOPIC")

# How many seconds before expiry to proactively refresh the token.
TOKEN_REFRESH_MARGIN = 30

class TokenManager:
    def __init__(self):
        self.token = None
        self.expires_at = None

    def get_token(self):
        """Return a valid access token, refreshing automatically if needed."""
        if self.token and self.expires_at and datetime.now() < self.expires_at:
            return self.token
        return self._refresh()

    def _refresh(self):
        """Fetch a new access token from the OpenSky authentication server."""
        r = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            },
        )
        r.raise_for_status()

        data = r.json()
        self.token = data["access_token"]
        expires_in = data.get("expires_in", 1800)
        self.expires_at = datetime.now() + timedelta(seconds=expires_in - TOKEN_REFRESH_MARGIN)
        return self.token

    def headers(self):
        """Return request headers with a valid Bearer token."""
        return {"Authorization": f"Bearer {self.get_token()}"}

# Use 'states/all' for real-time tracking, NOT 'flights/all'
url = "https://opensky-network.org/api/states/all"

tokens = TokenManager()
tokens._refresh()

# Optional: Add a bounding box to only grab flights over South America 
# format: (min_latitude, min_longitude, max_latitude, max_longitude)
params = {
    "lamin": -34.0,  
    "lomin": -74.0,  
    "lamax": 5.0,    
    "lomax": -34.0   
}

# Free OpenSky accounts can poll every 30 seconds
POLL_INTERVAL = 30

publisher = pubsub_v1.PublisherClient()

for i in range(1):
    # If authenticated, pass auth=(username, password) inside requests.get()
    response = requests.get(url, params=params, headers=tokens.headers())

    try:
        response.raise_for_status()
        response.json()
    except Exception as e:
        logger.error(f"{str(e)} when fetching API data")

    if response.status_code == 200:

        flight_data = response.json()
        states = flight_data.get("states", [])
        
        for flight in states:
            
            payload = {
                "icao24": flight[0],
                "callsign": flight[1],
                "country": flight[2],
                "time_position": flight[3],
                "last_contact": flight[4] if flight[4] is not None else int(datetime.now().timestamp()),
                "longitude": flight[5],
                "latitude": flight[6],
                "baro_altitude": flight[7],
                "on_ground": flight[8],
                "velocity": flight[9], # in m/s
                "true_track": flight[10],
                "vertical_rate": flight[11],
                "sensors" : flight[12],
                "geo_altitude" : flight[13],
                "squawk" : flight[14],
                "spi" : flight[15],
                "position_source" : flight[16],

                # Flag to track the ingestion timestamp
                "ingestion_timestamp": int(datetime.now().timestamp()),

                # Flag that doesn't exist in the api, True if the message has 
                # a non-null 'last_contact' field 
                "has_valid_last_contact" : True if flight[4] is not None else False,
            }

            future = publisher.publish(PUB_SUB_TOPIC, json.dumps(payload).encode('utf-8'),
                                       # converts to miliseconds here (cause of beams's api)
                                       last_contact=str(int(payload["last_contact"])*1000)) 

            try:
                message_id = future.result(timeout=60)
                logger.info(f"Successfully published message. ID: {message_id}")
            except Exception as e:
                logger.error(f"Pub/Sub publishing failed: {str(e)}")

        logger.info(f"\nTotal number of messages: {len(states)}")

    time.sleep(POLL_INTERVAL)

