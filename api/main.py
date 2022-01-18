from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import json

# Global vars
spark = SparkSession.builder.appName("main").master("local[*]").getOrCreate()

def init():
    global df_appearances, df_clubs, df_competitions, df_games, df_leagues, df_players
    '''appearences = sc.textFile("gs://data_football/appearances.csv")
    list_appearences = appearences.collect()
    for a in list_appearences:
        print(a)
    df_cards = spark.read.csv('https://storage.cloud.com/data_football/appearances.csv', inferSchema=True, header=True, sep=',')'''
    df_appearances = spark.read.csv('data/appearances.csv', inferSchema=True, header=True, sep=',')
    df_clubs = spark.read.csv('data/clubs.csv', inferSchema=True, header=True, sep=',')
    df_competitions = spark.read.csv('data/competitions.csv', inferSchema=True, header=True, sep=',')
    df_games = spark.read.csv('data/games.csv', inferSchema=True, header=True, sep=',')
    df_leagues = spark.read.csv('data/leagues.csv', inferSchema=True, header=True, sep=',')
    df_players = spark.read.csv('data/players.csv', inferSchema=True, header=True, sep=',')
    


def jugador_resumen():
    df_appearances.createOrReplaceTempView('sqlAppeareances')
    df_players.createOrReplaceTempView('sqlPlayers')
    
    result = spark.sql(''' SELECT FIRST(sqlAppeareances.player_id) AS player_id, sqlPlayers.name, SUM(sqlAppeareances.goals), SUM(sqlAppeareances.assists), round(AVG(sqlAppeareances.minutes_played), 2) AS minutes_played FROM sqlPlayers 
    JOIN sqlAppeareances ON sqlPlayers.player_id = sqlAppeareances.player_id GROUP BY sqlPlayers.name ORDER BY player_id''').show()#.toJSON().collect()
    return json.dumps(result)


if __name__ == "__main__":
    init()
    jugador_resumen()