import streamlit as st
import pandas as pd
import requests
import json

foot_selected = "Left"
position_selected = "Midfield"

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
    r_foot = requests.get('http://127.0.0.1:5000/api/get_data_foot').content
    r_position = requests.get('http://127.0.0.1:5000/api/get_data_position').content
    init_foot(r_foot)
    init_position(r_position)

def seleccion_inglesa_table(data):
    st.header('11 Jugadores ingleses')
    res = load_json_data(data)
    df = pd.DataFrame(res).astype(str)
    st.write(df)

def jugador_resume_table(data):
    st.header('Información general jugadores')
    res = load_json_data(data)
    df = pd.DataFrame(res).astype(str)
    st.write(df)

def most_expensive_players(data):
    res = load_json_data(data)
    st.write(res)

def roaster_value(data):
    res = load_json_data(data)
    st.write(res)

def local_victories(data):
    res = load_json_data(data)
    st.write(res)

def mainpage_requests():
    r_jugador_resume = requests.get(url='http://127.0.0.1:5000/api/jugador_resume').content
    r_seleccion_inglesa = requests.get(url='http://127.0.0.1:5000/api/seleccion_inglesa').content
    r_most_expensive_players = requests.get(url='http://127.0.0.1:5000/api/most_expensive_players').content
    r_roaster_value = requests.get(url='http://127.0.0.1:5000/api/roaster_value').content
    r_local_victories = requests.get(url='http://127.0.0.1:5000/api/local_victories').content

    jugador_resume_table(r_jugador_resume)
    seleccion_inglesa_table(r_seleccion_inglesa)
    most_expensive_players(r_most_expensive_players)
    roaster_value(r_roaster_value)
    local_victories(r_local_victories)

def jugador_pos_request():
    requests.post(url='http://127.0.0.1:5000/api/foot_selected', data=json.dumps({'foot': foot_selected}))
    requests.post(url='http://127.0.0.1:5000/api/position_selected', data=json.dumps({'position': position_selected}))
    r_jugador_position_foot_price = requests.get(url='http://127.0.0.1:5000/api/jugador_position_foot_price').content
    jugador_position_foot_price_table(r_jugador_position_foot_price)
    

if __name__ == '__main__':
    try:
        #settings_st()
        data_request_foot_position()
        jugador_pos_request()
        mainpage_requests()
    except requests.exceptions.ConnectionError:
        print('Connection refused, try again later...')