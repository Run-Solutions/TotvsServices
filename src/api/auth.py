import time
import requests
from requests_oauthlib import OAuth1
from config.settings import settings
from utils.logger import logger

class OAuth2Manager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(OAuth2Manager, cls).__new__(cls)
            cls._instance._init()
        return cls._instance

    def _init(self):
        self.token_url = settings.API_TOKEN_URL
        self.username = settings.API_USERNAME
        self.password = settings.API_PASSWORD
        self.consumer_key = settings.API_CONSUMER_KEY
        self.consumer_secret = settings.API_CONSUMER_SECRET
        
        self.access_token = None
        self.refresh_token = None
        self.expires_at = 0

    def get_token(self):
        """Returns a valid access token, refreshing or fetching a new one if necessary."""
        if self._is_token_valid():
            return self.access_token
        
        if self.refresh_token:
            if self._refresh_access_token():
                return self.access_token

        self._fetch_new_token()
        return self.access_token

    def _is_token_valid(self):
        """Checks if the current access token is valid with a buffer time."""
        # Buffer of 60 seconds
        return self.access_token and time.time() < (self.expires_at - 60)

    def _fetch_new_token(self):
        """Fetches a new access token using password grant and OAuth 1.0 signature."""
        logger.info("Fetching new access token...")
        try:
            # Prepare OAuth 1.0 signature for client authentication
            auth = OAuth1(self.consumer_key, self.consumer_secret)
            
            data = {
                "grant_type": "password",
                "username": self.username,
                "password": self.password
            }
            
            response = requests.post(self.token_url, auth=auth, data=data)
            response.raise_for_status()
            
            self._update_tokens(response.json())
            logger.info("Successfully obtained new access token.")
            
        except Exception as e:
            logger.error(f"Error fetching access token: {e}")
            raise

    def _refresh_access_token(self):
        """Refreshes the access token using the refresh token."""
        logger.info("Refreshing access token...")
        try:
            # Assuming refresh usually requires client authentication too
            auth = OAuth1(self.consumer_key, self.consumer_secret)
            
            data = {
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token
            }
            
            response = requests.post(self.token_url, auth=auth, data=data) 
            if response.status_code != 200:
                logger.warning(f"Failed to refresh token: {response.text}")
                return False
                
            self._update_tokens(response.json())
            logger.info("Successfully refreshed access token.")
            return True
            
        except Exception as e:
            logger.error(f"Error refreshing token: {e}")
            return False

    def _update_tokens(self, token_data):
        self.access_token = token_data.get("access_token")
        self.refresh_token = token_data.get("refresh_token")
        expires_in = token_data.get("expires_in", 3600)
        self.expires_at = time.time() + int(expires_in)
