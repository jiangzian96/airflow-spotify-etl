import spotipy
import spotipy.util as util
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.oauth2 import SpotifyOAuth


def _get_api_token():
    CLIENT_ID = '22b510b0e20b4ab7934e99a4db5aa5dd'
    CLIENT_SECRET = 'ac0ddb7dc24c43abb613c7c97caff875'
    USER_ID = '12177044118'
    scope = "user-read-recently-played"
    token = util.prompt_for_user_token(USER_ID, scope, client_id=CLIENT_ID, client_secret=CLIENT_SECRET, redirect_uri='http://localhost:8000/')
    print(token)
    
_get_api_token()