from pyspark.sql import SparkSession
import json


spark = SparkSession.builder.appName("main").master("local[*]").getOrCreate() # Initialize spark
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile","data/gvd-rvr-3b8dd1e2d238.json") # Service creation (with json keyfile) for accessing bucket

def load_data_local():
    global df_appearances, df_clubs, df_competitions, df_games, df_leagues, df_players
    df_appearances = spark.read.csv('data/appearances.csv', inferSchema=True, header=True, sep=',')
    df_clubs = spark.read.csv('data/clubs.csv', inferSchema=True, header=True, sep=',')
    df_competitions = spark.read.csv('data/competitions.csv', inferSchema=True, header=True, sep=',')
    df_games = spark.read.csv('data/games.csv', inferSchema=True, header=True, sep=',')
    df_leagues = spark.read.csv('data/leagues.csv', inferSchema=True, header=True, sep=',')
    df_players = spark.read.csv('data/players.csv', inferSchema=True, header=True, sep=',')
    
def load_data_bucket():
    global df_appearances, df_clubs, df_competitions, df_games, df_leagues, df_players
    bucket_name='data_football'
    df_appearances = spark.read.csv(f"gs://{bucket_name}/appearances.csv", inferSchema=True, header=True, sep=',')
    df_clubs = spark.read.csv(f"gs://{bucket_name}/clubs.csv", inferSchema=True, header=True, sep=',')
    df_competitions = spark.read.csv(f"gs://{bucket_name}/competitions.csv", inferSchema=True, header=True, sep=',')
    df_games = spark.read.csv(f"gs://{bucket_name}/games.csv", inferSchema=True, header=True, sep=',')
    df_leagues = spark.read.csv(f"gs://{bucket_name}/leagues.csv", inferSchema=True, header=True, sep=',')
    df_players = spark.read.csv(f"gs://{bucket_name}/players.csv", inferSchema=True, header=True, sep=',') 

def jugador_resumen():
    df_appearances.createOrReplaceTempView('sqlAppeareances')
    df_players.createOrReplaceTempView('sqlPlayers')
    result = spark.sql(''' SELECT FIRST(sqlAppeareances.player_id) AS player_id, sqlPlayers.name, SUM(sqlAppeareances.goals), SUM(sqlAppeareances.assists), round(AVG(sqlAppeareances.minutes_played), 2) AS minutes_played FROM sqlPlayers 
    JOIN sqlAppeareances ON sqlPlayers.player_id = sqlAppeareances.player_id GROUP BY sqlPlayers.name ORDER BY player_id''').show()#.toJSON().collect()
    return json.dumps(result)

if __name__ == "__main__":
    load_data_bucket()
    jugador_resumen()