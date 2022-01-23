from pyspark.sql import SparkSession
import json
from flask import Flask, request
from flask_cors import CORS

# Global variables
foot = "Left"
position = "Midfield"
pretty_name = "1 FC Koln"

spark = SparkSession.builder \
    .appName('main') \
    .master('local[*]') \
    .getOrCreate() 
# Service creation (with json keyfile) to access google cloud bucket
spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')
spark._jsc.hadoopConfiguration().set('google.cloud.auth.service.account.json.keyfile', 'data/gvd-rvr-3b8dd1e2d238.json') 
df_appearances, df_clubs, df_competitions, df_games, df_leagues, df_players, df_final = None, None, None, None, None, None, None
app = Flask(__name__)
CORS(app)

# Load data from 'data' folder
def load_data_local():
    global df_appearances, df_clubs, df_competitions, df_games, df_leagues, df_players, df_final
    df_appearances = spark.read.csv('data/appearances.csv', inferSchema=True, header=True, sep=',')
    df_clubs = spark.read.csv('data/clubs.csv', inferSchema=True, header=True, sep=',')
    df_competitions = spark.read.csv('data/competitions.csv', inferSchema=True, header=True, sep=',')
    df_games = spark.read.csv('data/games.csv', inferSchema=True, header=True, sep=',')
    df_leagues = spark.read.csv('data/leagues.csv', inferSchema=True, header=True, sep=',')
    df_players = spark.read.csv('data/players.csv', inferSchema=True, header=True, sep=',')
    df_final = spark.read.csv('data/Final.csv', inferSchema=True, header=True, sep=';')

# Load data from google cloud bucket
def load_data_bucket():
    global df_appearances, df_clubs, df_competitions, df_games, df_leagues, df_players, df_final
    bucket_name='data_football'
    df_appearances = spark.read.csv(f'gs://{bucket_name}/appearances.csv', inferSchema=True, header=True, sep=',')
    df_clubs = spark.read.csv(f'gs://{bucket_name}/clubs.csv', inferSchema=True, header=True, sep=',')
    df_competitions = spark.read.csv(f'gs://{bucket_name}/competitions.csv', inferSchema=True, header=True, sep=',')
    df_games = spark.read.csv(f'gs://{bucket_name}/games.csv', inferSchema=True, header=True, sep=',')
    df_leagues = spark.read.csv(f'gs://{bucket_name}/leagues.csv', inferSchema=True, header=True, sep=',')
    df_players = spark.read.csv(f'gs://{bucket_name}/players.csv', inferSchema=True, header=True, sep=',') 
    df_final = spark.read.csv(f'gs://{bucket_name}/Final.csv', inferSchema=True, header=True, sep=';')

''' Show player name, total goals and assists and a media of minutes played '''
@app.route('/api/jugador_resume', methods=['GET'])
def jugador_resume():
    df_appearances.createOrReplaceTempView('sqlAppeareances')
    df_players.createOrReplaceTempView('sqlPlayers')
    result = spark.sql(''' 
    SELECT FIRST(sqlAppeareances.player_id) AS player_id, sqlPlayers.name, SUM(sqlAppeareances.goals), SUM(sqlAppeareances.assists), 
           round(AVG(sqlAppeareances.minutes_played), 2) AS media_minutes_played, COUNT(sqlPlayers.player_id) AS games_played 
    FROM sqlPlayers 
    JOIN sqlAppeareances ON sqlPlayers.player_id = sqlAppeareances.player_id 
    GROUP BY sqlPlayers.name 
    ORDER BY player_id ''').toJSON().collect()
    return json.dumps(result)

''' Take players where foot and position is introduced by the user '''
@app.route('/api/jugador_position_foot_price', methods=['GET'])
def jugador_position_foot_price():
    df_players.createOrReplaceTempView('sqlPlayers')
    df_final.createOrReplaceTempView('sqlFinal')
    result = spark.sql(''' 
    SELECT sqlPlayers.pretty_name, sqlPlayers.position, sqlPlayers.foot, sqlFinal.Player, sqlFinal.Market_value, sqlFinal.Club_x 
    FROM sqlPlayers 
    JOIN sqlFinal ON sqlPlayers.pretty_name = sqlFinal.Player 
    WHERE position = \"{}\" AND foot = \"{}\" '''.format(position, foot)).toJSON().collect()
    return json.dumps(result)

''' Get the different values for "foot" variable '''
@app.route('/api/get_data_foot', methods=['GET'])
def get_data_foot():
    rows = df_players.select('foot').distinct().collect()
    return json.dumps([row['foot'] for row in rows])

''' Take value of "foot" from selectBox '''
@app.route('/api/foot_selected', methods=['POST'])
def foot_selected():
    global foot
    request_data = json.loads(request.data)
    foot = request_data['foot']
    return json.dumps({'message': 'success'})

''' Get the different values for "position" variable '''
@app.route('/api/get_data_position', methods=['GET'])
def get_data_position():
    rows = df_players.select('position').distinct().orderBy('position', ascending=False).collect()
    return json.dumps([row['position'] for row in rows])

''' Take value of "position" from selectBox '''
@app.route('/api/position_selected', methods=['POST'])
def position_selected():
    global position
    request_data = json.loads(request.data)
    position = request_data['position']
    return json.dumps({'message': 'success'})

''' Take players from England of each position with their market price value  ''' 
@app.route('/api/seleccion_inglesa', methods=['GET'])
def seleccion_inglesa():
    df_players.createOrReplaceTempView('sqlPlayers')
    df_final.createOrReplaceTempView('sqlFinal')
    result = spark.sql(''' 
    SELECT FIRST(sqlPlayers.pretty_name) AS pretty_name, FIRST(sqlPlayers.sub_position), FIRST(sqlFinal.Market_value), FIRST(sqlFinal.Nation) 
    FROM sqlPlayers 
    JOIN sqlFinal ON sqlPlayers.pretty_name = sqlFinal.Player 
    WHERE Nation = "ENG" 
    GROUP BY sqlPlayers.sub_position''').toJSON().collect()
    return json.dumps(result)

''' Displays the 15 most expensive players (biggest market value) with its respective market value '''
@app.route('/api/most_expensive_players', methods=['GET'])
def most_expensive_per_position():
    df_players.createOrReplaceTempView('players')
    df_clubs.createOrReplaceTempView('clubs')
    query = spark.sql('''
        SELECT players.pretty_name AS Name, highest_market_value_in_gbp AS Market_Value
        FROM players
        JOIN clubs ON clubs.club_id = players.current_club_id
        ORDER BY highest_market_value_in_gbp DESC
        LIMIT 15
    ''').toJSON().collect()
    return json.dumps(query)

''' Team name, number of players, team value and average of team value '''
@app.route('/api/roaster_value', methods=['GET'])
def roaster_value():
    df_clubs.createOrReplaceTempView('sqlClubs')
    result = spark.sql(''' SELECT sqlClubs.pretty_name, sqlClubs.squad_size, sqlClubs.total_market_value, sqlClubs.total_market_value / sqlClubs.squad_size AS avg_player_value
    FROM sqlClubs
    WHERE squad_size != 0    
    ORDER BY squad_size ASC
    ''').toJSON().collect()
    return json.dumps(result)

''' Results of all games won as local '''
@app.route('/api/local_victories', methods=['GET'])
def local_victories():
    df_clubs.createOrReplaceTempView('sqlClubs')
    df_games.createOrReplaceTempView('sqlGames')
    result = spark.sql(''' SELECT home_club.pretty_name AS home_club, away_club.pretty_name AS away_club, sqlGames.home_club_goals AS Goles_local, 
        sqlGames.away_club_goals AS Goles_visitante
        FROM sqlGames
        JOIN sqlClubs home_club ON sqlGames.home_club_id = home_club.club_id
        JOIN sqlClubs away_club ON sqlGames.away_club_id = away_club.club_id
        WHERE home_club.pretty_name =\"{}\" AND sqlGames.away_club_goals < sqlGames.home_club_goals '''.format(pretty_name)).toJSON().collect()
    return json.dumps(result)

''' Take top scoring club from each different competition '''
@app.route('/api/top_scoring_clubs', methods=['GET'])
def top_scoring_clubs():
    df_clubs.createOrReplaceTempView('sqlClubs')
    df_games.createOrReplaceTempView('sqlGames')
    df_leagues.createOrReplaceTempView('sqlLeagues')
    result = spark.sql(''' 
        SELECT sqlLeagues.league_id, FIRST(sqlLeagues.name) AS Name_league, 
        SUM(sqlGames.away_club_goals) AS Total_goals_away, FIRST(sqlClubs.pretty_name) AS Name_club 
        FROM sqlLeagues
        JOIN sqlGames ON sqlLeagues.league_id = sqlGames.competition_code
        JOIN sqlClubs ON sqlGames.away_club_id = sqlClubs.club_id
        GROUP BY sqlLeagues.league_id
    ''').toJSON().collect()
    return json.dumps(result)

''' Returns the total amount of european leagues ''' 
@app.route('/api/leagues', methods=['GET'])
def leagues():
    df_leagues.createOrReplaceTempView('leagues')
    query = spark.sql('''
        SELECT name, league_id 
        FROM leagues
    ''').toJSON().collect()
    return json.dumps(query)

''' Returns domestic cups  '''
@app.route('/api/domestic_cups', methods=['GET'])
def domestic_cups():
    df_competitions.createOrReplaceTempView('competitions')
    query = spark.sql('''
        SELECT name, country_name
        FROM competitions
        WHERE type = "domestic_cup"
    ''').toJSON().collect()
    return json.dumps(query)

''' Get the different team names for "sqlClubs.pretty_name" variable '''
@app.route('/api/get_data_club_name', methods=['GET'])
def get_data_club_name():
    rows = df_clubs.select('pretty_name').distinct().orderBy('pretty_name', ascending=True).collect()
    return json.dumps([row['pretty_name'] for row in rows])

''' Take value of "pretty_name" from selectBox '''
@app.route('/api/pretty_name_selected', methods=['POST'])
def pretty_name_selected():
    global pretty_name
    request_data = json.loads(request.data)
    pretty_name = request_data['pretty_name']
    return json.dumps({"message": "success"})

if __name__ == '__main__':
    load_data_bucket()
    app.run(debug=True)