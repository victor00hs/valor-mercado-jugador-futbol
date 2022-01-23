import streamlit as st
import pandas as pd
import requests
import json
import plotly.graph_objects as go
import plotly.express as px

# Global vars
foot_selected = "Left"
position_selected = "Midfield"
club_name_selected = "1 FC Koln"

# Define page settings
def settings_st():
    # Page settings
    st.set_page_config(
        page_title="Grandes Volumenes de Datos",
        page_icon="💻",
        layout="wide",
        initial_sidebar_state="auto",
        menu_items={'About':"""
            ## Authors: 
                Ramón Íñiguez Bascuas\n
                Víctor Hernández Sanz\n
                Rubén Ortiz Nieto\n
            [Link to Github repository](https://github.com/victor00hs/valor-mercado-jugador-futbol)"""}
    )
    st.title('Información general sobre el mercado')

# Loads json data from API response into a serializable format
def load_json_data(data):
    result = json.loads(data)
    for idx, i in enumerate(result):
        result[idx] = json.loads(i)
    return result

# Load general data 
def row1(data_col1, data_col2):
    res_col1, res_col2 = load_json_data(data_col1), load_json_data(data_col2)
    df_col1, df_col2 = pd.DataFrame(res_col1).astype(str), pd.DataFrame(res_col2).astype(str)

    st.header('Todas las ligas')
    st.write('A continuacion se muestran todas las ligas disponibles.')
    fig = px.pie(
        hole = 0.1,
        names = df_col1['name'],
        labels = df_col1['league_id']
    )
    st.plotly_chart(fig)

    st.header('Copas domesticas')
    st.write('A continuacion se muestran el numero de copas domesticas disponibles.')
    fig2 = px.pie(
        names = df_col2['name'],
        labels = df_col2['country_name']
    )
    st.plotly_chart(fig2)

# Shows players and price with "foot" and "position" given by the user
def jugador_position_foot_price_table(data):
    st.header('Jugadores seleccionando "Pie bueno" y "Posición"')
    st.write('En esta tabla el usuario puede elegir el pie bueno y la posición de juego para todos los jugadores dentro del dataset de jugadores.')
    res = load_json_data(data)
    df = pd.DataFrame(res).astype(str)
    st.write(df)

# Fill Selectbox with "foot" choices
def init_foot(foot):
    global foot_selected
    foot_selected = st.selectbox('Selecciona el pie de preferencia', json.loads(foot))

# Fill Selectbox with "position" choices
def init_position(position):
    global position_selected
    position_selected = st.selectbox('Selecciona la posicion del jugador', json.loads(position))

# GET request calls for "jugador_position_foot_price_table"
def data_request_foot_position():
    st.header('Selector de variables')
    r_foot = requests.get('http://127.0.0.1:5000/api/get_data_foot').content
    r_position = requests.get('http://127.0.0.1:5000/api/get_data_position').content
    init_foot(r_foot)
    init_position(r_position)

# Explicar esto @victor00hs
def seleccion_inglesa_table(data):
    st.header('11 Jugadores ingleses')
    st.write('En esta tabla se muestran 11 jugadores ingleses de todas las sub posiciones junto al precio de cada uno.')
    res = load_json_data(data)
    df = pd.DataFrame(res).astype(str)
    st.write(df)

# Explicar esto @victor00hs
def jugador_resume_table(data):
    st.header('Información general jugadores')
    st.write('En esta tabla se muestran todos los jugadores ordenados por ID junto a la suma de goles, suma de asistencias y una media de los minutos jugados por partido.')
    res = load_json_data(data)
    df = pd.DataFrame(res).astype(str)
    st.write(df)

# Shows the most expensive players with its name & value using a bar chart
def most_expensive_players(data):
    res = load_json_data(data)
    df = pd.DataFrame(res).astype(str)
    df['Market_Value'] = pd.to_numeric(df['Market_Value'])
    st.header('Jugadores más caros del mercado')
    st.write('A continuacion se muestran los 15 jugadores más caros del mercado.')

    fig = px.bar(
        df, 
        x = 'Market_Value',
        y = 'Name',
        color = 'Name'
    )
    st.plotly_chart(fig)

# Explica esto @victor00hs
def roaster_value(data):
    st.header('Equipo con el total de jugadores, su valor y la media de su valor')
    st.write('En esta tabla se muestran los equipos con la cantidad de jugadores, el valor de la totalidad del equipo y también la media del valor del equipo.')
    res = load_json_data(data)
    df = pd.DataFrame(res).astype(str)
    st.write(df)

# Shows local matches won of a team introduced by user
def local_victories_table(data):
    st.header('Resultado de todos los partidos ganados como local')
    st.write('En esta tabla se muestran los resultados de los partidos ganados del equipo que el usuario busca jugado como local, además del ID del equipo contra el que disputo el encuentro.')
    res = load_json_data(data)
    df = pd.DataFrame(res).astype(str)
    st.write(df)

# Fill Selectbox with "team name" choices
def init_data_club_name(pretty_name):
    global club_name_selected
    club_name_selected = st.selectbox('Selecciona el equipo para buscar contenido', json.loads(pretty_name))

# GET request calls for "local_victories_table"
def data_request_club_name():
    st.header('Selector de variables')
    r_data_club_name = requests.get('http://127.0.0.1:5000/api/get_data_club_name').content
    init_data_club_name(r_data_club_name)

# We define all the mainpage requests here
def mainpage_requests():
    r_leagues = requests.get(url='http://127.0.0.1:5000/api/leagues').content
    r_domestic_cups = requests.get(url='http://127.0.0.1:5000/api/domestic_cups').content
    r_most_expensive_players = requests.get(url='http://127.0.0.1:5000/api/most_expensive_players').content
    r_jugador_resume = requests.get(url='http://127.0.0.1:5000/api/jugador_resume').content
    r_seleccion_inglesa = requests.get(url='http://127.0.0.1:5000/api/seleccion_inglesa').content
    r_roaster_value = requests.get(url='http://127.0.0.1:5000/api/roaster_value').content

    row1(r_leagues, r_domestic_cups)
    most_expensive_players(r_most_expensive_players)
    jugador_resume_table(r_jugador_resume)
    seleccion_inglesa_table(r_seleccion_inglesa)
    roaster_value(r_roaster_value)

# We define player options selection requests
def jugador_pos_request():
    requests.post(url='http://127.0.0.1:5000/api/foot_selected', data=json.dumps({'foot': foot_selected}))
    requests.post(url='http://127.0.0.1:5000/api/position_selected', data=json.dumps({'position': position_selected}))
    r_jugador_position_foot_price = requests.get(url='http://127.0.0.1:5000/api/jugador_position_foot_price').content
    jugador_position_foot_price_table(r_jugador_position_foot_price)

# Request with "local_victories_table" funtion information
def pretty_name_request():
    requests.post(url='http://127.0.0.1:5000/api/pretty_name_selected', data=json.dumps({'pretty_name': club_name_selected}))
    r_local_victories = requests.get(url='http://127.0.0.1:5000/api/local_victories').content
    local_victories_table(r_local_victories)

def general_info_requests():
    requests.get()

if __name__ == '__main__':
    settings_st()
    try:
        mainpage_requests()
        data_request_foot_position()
        jugador_pos_request()
        data_request_club_name()
        pretty_name_request()
    except requests.exceptions.ConnectionError:
        print('Connection refused, try again later...')