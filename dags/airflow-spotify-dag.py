from airflow.models import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import datetime
import spotipy
import spotipy.util as util
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd


default_args = {
    'start_date': datetime.datetime(2021, 1, 1)
}


def _get_api_token(ti):
    CLIENT_ID = '22b510b0e20b4ab7934e99a4db5aa5dd'
    CLIENT_SECRET = 'ac0ddb7dc24c43abb613c7c97caff875'
    USER_ID = '12177044118'
    scope = "user-read-recently-played"
    token = util.prompt_for_user_token(USER_ID, scope, client_id=CLIENT_ID, client_secret=CLIENT_SECRET, redirect_uri='http://localhost:8000/')
    ti.xcom_push(key="api_token", value=token)
    

def _read_recently_played(ti):
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
    token = ti.xcom_pull(key="api_token", task_ids="get_token")
    spotify = spotipy.Spotify(auth=token)
    d = spotify.current_user_recently_played(after=yesterday_unix_timestamp)
    ti.xcom_push(key="raw_data", value=d)
    

def _process_recently_played(ti):
    data = ti.xcom_pull(key="raw_data", task_ids="read_data")
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []
    
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])
        
    song_dict = {
        "song_name" : song_names,
        "artist_name": artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }
    
    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])
    song_df.to_csv("/tmp/processed_data.csv", index=None, header=False)
    

with DAG("airflow-spotify", schedule_interval="@daily",
         default_args=default_args,
         catchup=False) as dag:
    
    create_table = SqliteOperator(
        task_id="create_table",
        sqlite_conn_id="db_sqlite",
        sql="""
        CREATE TABLE IF NOT EXISTS my_played_tracks(
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            played_at VARCHAR(200),
            timestamp VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    );                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
    """
    )

    get_token = PythonOperator(
        python_callable=_get_api_token,
        task_id="get_token"
    )
    
    read_data = PythonOperator(
        python_callable=_read_recently_played,
        task_id="read_data"
    )
    
    process_data = PythonOperator(
        python_callable=_process_recently_played,
        task_id="process_data"
    )
    
    store_data = BashOperator(
        task_id="store_data",
        bash_command='echo -e ".separator ","\n.import /tmp/processed_data.csv my_played_tracks" | sqlite3 /Users/zianjiang/airflow/airflow.db'
    )
    
create_table >> get_token >> read_data >> process_data >> store_data