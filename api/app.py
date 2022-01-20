from pyspark.sql import SparkSession
import json
from flask import Flask, request
from flask_cors import CORS

#Global variables
spark = SparkSession.builder.appName("main").master("local[*]").getOrCreate() # Initialize spark
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile","data/gvd-rvr-3b8dd1e2d238.json") # Service creation (with json keyfile) for accessing bucket
df_appearances, df_clubs, df_competitions, df_games, df_leagues, df_players, df_final = None, None, None, None, None, None, None
app = Flask(__name__)
CORS(app)

#Function that loads data from "data" folder
def load_data_local():
    global df_appearances, df_clubs, df_competitions, df_games, df_leagues, df_players, df_final
    df_appearances = spark.read.csv('data/appearances.csv', inferSchema=True, header=True, sep=',')
    df_clubs = spark.read.csv('data/clubs.csv', inferSchema=True, header=True, sep=',')
    df_competitions = spark.read.csv('data/competitions.csv', inferSchema=True, header=True, sep=',')
    df_games = spark.read.csv('data/games.csv', inferSchema=True, header=True, sep=',')
    df_leagues = spark.read.csv('data/leagues.csv', inferSchema=True, header=True, sep=',')
    df_players = spark.read.csv('data/players.csv', inferSchema=True, header=True, sep=',')
    df_final = spark.read.csv('data/Final.csv', inferSchema=True, header=True, sep=';')

#Function that loads data from Google bucket
def load_data_bucket():
    global df_appearances, df_clubs, df_competitions, df_games, df_leagues, df_players, df_final
    bucket_name='data_football'
    df_appearances = spark.read.csv(f"gs://{bucket_name}/appearances.csv", inferSchema=True, header=True, sep=',')
    df_clubs = spark.read.csv(f"gs://{bucket_name}/clubs.csv", inferSchema=True, header=True, sep=',')
    df_competitions = spark.read.csv(f"gs://{bucket_name}/competitions.csv", inferSchema=True, header=True, sep=',')
    df_games = spark.read.csv(f"gs://{bucket_name}/games.csv", inferSchema=True, header=True, sep=',')
    df_leagues = spark.read.csv(f"gs://{bucket_name}/leagues.csv", inferSchema=True, header=True, sep=',')
    df_players = spark.read.csv(f"gs://{bucket_name}/players.csv", inferSchema=True, header=True, sep=',') 
    df_final = spark.read.csv(f"gs://{bucket_name}/Final.csv", inferSchema=True, header=True, sep=';')

''' Show player name, total goals and assists and a media of minutes played '''
@app.route('/api/jugador_resume', methods=['GET'])
def jugador_resume():
    df_appearances.createOrReplaceTempView('sqlAppeareances')
    df_players.createOrReplaceTempView('sqlPlayers')
    result = spark.sql(''' SELECT FIRST(sqlAppeareances.player_id) AS player_id, sqlPlayers.name, SUM(sqlAppeareances.goals), SUM(sqlAppeareances.assists), round(AVG(sqlAppeareances.minutes_played), 2) AS media_minutes_played, COUNT(sqlPlayers.player_id) AS games_played FROM sqlPlayers 
    JOIN sqlAppeareances ON sqlPlayers.player_id = sqlAppeareances.player_id GROUP BY sqlPlayers.name ORDER BY player_id''').toJSON().collect()
    return json.dumps(result)

def jugador_position_foot_price():
    df_players.createOrReplaceTempView('sqlPlayers')
    df_final.createOrReplaceTempView('sqlFinal')
    result = spark.sql(''' SELECT sqlPlayers.pretty_name, sqlPlayers.position, sqlPlayers.foot, sqlFinal.Player, sqlFinal.Market_value, sqlFinal.Club_x FROM sqlPlayers 
    JOIN sqlFinal ON sqlPlayers.pretty_name = sqlFinal.Player WHERE position = "Midfield" AND foot = "Right" ''').show()
    return json.dumps(result)

if __name__ == "__main__":
    load_data_bucket()
    app.run(debug=True)