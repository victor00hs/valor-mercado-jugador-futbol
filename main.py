import streamlit as st
import pandas as pd
import requests
import json
from  api.predictions import *

foot_selected = "Left"
position_selected = "Midfield"
club_name_selected = "1 FC Koln"

# Load css settings
def local_css(file_name):
    with open(file_name) as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

def settings_st():
    # Page settings
    st.set_page_config(
    page_title="Grandes Volumenes de Datos",
    page_icon="💻",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={'About':"""
        ## Authors: 
            Ramón Íñiguez Bascuas\n
            Víctor Hernández Sanz\n
            Rubén Ortiz Nieto\n
        [Link to Github repository](https://github.com/victor00hs/valor-mercado-jugador-futbol)"""}
    )
    st.title('Valor de mercado de un jugador de futbol')

# Explicar esta movie
def load_json_data(data):
    result = json.loads(data)
    for idx, i in enumerate(result):
        result[idx] = json.loads(i)
    return result

#Table that shows players and price with "foot" and "position" given by the user
def jugador_position_foot_price_table(data):
    st.header('Jugadores seleccionando "Pie bueno" y "Posición"')
    st.write('En esta tabla el usuario puede elegir el pie bueno y la posición de juego para todos los jugadores dentro del dataset de jugadores.')
    res = load_json_data(data)
    df = pd.DataFrame(res).astype(str)
    st.write(df)

#Fill Selectbox with "foot" choices
def init_foot(foot):
    global foot_selected
    foot_selected = st.selectbox('Selecciona el pie de preferencia', json.loads(foot))

#Fill Selectbox with "position" choices
def init_position(position):
    global position_selected
    position_selected = st.selectbox('Selecciona la posicion del jugador', json.loads(position))

#GET request calls for "jugador_position_foot_price_table"
def data_request_foot_position():
    st.header('Selector de variables')
    r_foot = requests.get('http://127.0.0.1:5000/api/get_data_foot').content
    r_position = requests.get('http://127.0.0.1:5000/api/get_data_position').content
    init_foot(r_foot)
    init_position(r_position)

def seleccion_inglesa_table(data):
    st.header('11 Jugadores ingleses')
    st.write('En esta tabla se muestran 11 jugadores ingleses de todas las sub posiciones junto al precio de cada uno.')
    res = load_json_data(data)
    df = pd.DataFrame(res).astype(str)
    st.write(df)

def jugador_resume_table(data):
    st.header('Información general jugadores')
    st.write('En esta tabla se muestran todos los jugadores ordenados por ID junto a la suma de goles, suma de asistencias y una media de los minutos jugados por partido.')
    res = load_json_data(data)
    df = pd.DataFrame(res).astype(str)
    st.write(df)

def most_expensive_players(data):
    res = load_json_data(data)
    df = pd.DataFrame(res).astype(str)
    df = df.set_index('Name')
    df['Market_Value'] = pd.to_numeric(df['Market_Value'])
    st.header('Jugadores más caros del mercado')
    st.write('En esta gráfica se puede ver los 15 jugadores más caros del mercado.')
    st.bar_chart(df)

def roaster_value(data):
    st.header('Equipo con el total de jugadores, su valor y la media de su valor')
    st.write('En esta tabla se muestran los equipos con la cantidad de jugadores, el valor de la totalidad del equipo y también la media del valor del equipo.')
    res = load_json_data(data)
    df = pd.DataFrame(res).astype(str)
    st.write(df)

#Table that shows local matches won of a team introduced by user
def local_victories_table(data):
    st.header('Resultado de todos los partidos ganados como local')
    st.write('En esta tabla se muestran los resultados de los partidos ganados del equipo que el usuario busca jugado como local, además del ID del equipo contra el que disputo el encuentro.')
    res = load_json_data(data)
    df = pd.DataFrame(res).astype(str)
    st.write(df)

#Fill Selectbox with "team name" choices
def init_data_club_name(pretty_name):
    global club_name_selected
    club_name_selected = st.selectbox('Selecciona el equipo para buscar contenido', json.loads(pretty_name))

#GET request calls for "local_victories_table"
def data_request_club_name():
    st.header('Selector de variables')
    r_data_club_name = requests.get('http://127.0.0.1:5000/api/get_data_club_name').content
    init_data_club_name(r_data_club_name)

def mainpage_requests():
    r_jugador_resume = requests.get(url='http://127.0.0.1:5000/api/jugador_resume').content
    r_seleccion_inglesa = requests.get(url='http://127.0.0.1:5000/api/seleccion_inglesa').content
    r_most_expensive_players = requests.get(url='http://127.0.0.1:5000/api/most_expensive_players').content
    r_roaster_value = requests.get(url='http://127.0.0.1:5000/api/roaster_value').content

    jugador_resume_table(r_jugador_resume)
    show_predictions()
    seleccion_inglesa_table(r_seleccion_inglesa)
    most_expensive_players(r_most_expensive_players)
    roaster_value(r_roaster_value)

#Request with "jugador_position_foot_price_table" function information
def jugador_pos_request():
    requests.post(url='http://127.0.0.1:5000/api/foot_selected', data=json.dumps({'foot': foot_selected}))
    requests.post(url='http://127.0.0.1:5000/api/position_selected', data=json.dumps({'position': position_selected}))
    r_jugador_position_foot_price = requests.get(url='http://127.0.0.1:5000/api/jugador_position_foot_price').content
    jugador_position_foot_price_table(r_jugador_position_foot_price)

#Request with "local_victories_table" funtion information
def pretty_name_request():
    requests.post(url='http://127.0.0.1:5000/api/pretty_name_selected', data=json.dumps({'pretty_name': club_name_selected}))
    r_local_victories = requests.get(url='http://127.0.0.1:5000/api/local_victories').content
    local_victories_table(r_local_victories)

if __name__ == '__main__':
    local_css('styles/style.css')
    # settings_st()
    try:
        #settings_st()
        data_request_foot_position()
        jugador_pos_request()
        mainpage_requests()
        data_request_club_name()
        pretty_name_request()
    except requests.exceptions.ConnectionError:
        print('Connection refused, try again later...')