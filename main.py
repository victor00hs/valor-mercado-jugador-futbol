import streamlit as st
import pandas as pd
import requests
import json

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
    st.header('Informacion jugador (foot) (position) (price)')
    res = load_json_data(data)
    df = pd.DataFrame(res).astype(str)
    st.write(df)

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

def mainpage_requests():
    r_jugador_resume = requests.get(url='http://127.0.0.1:5000/api/jugador_resume').content
    r_jugador_position_foot_price = requests.get(url='http://127.0.0.1:5000/api/jugador_position_foot_price').content
    r_seleccion_inglesa = requests.get(url='http://127.0.0.1:5000/api/seleccion_inglesa').content
    r_most_expensive_players = requests.get(url='http://127.0.0.1:5000/api/most_expensive_players').content

    jugador_resume_table(r_jugador_resume)
    jugador_position_foot_price_table(r_jugador_position_foot_price)
    seleccion_inglesa_table(r_seleccion_inglesa)
    most_expensive_players(r_most_expensive_players)


if __name__ == '__main__':
    settings_st()
    try:
        mainpage_requests()
    except requests.exceptions.ConnectionError:
        print('Connection refused, try again later...')