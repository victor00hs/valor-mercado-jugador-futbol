import re
import streamlit as st
import pandas as pd
import requests
import json
   
def jugador_resume_table(data):
    st.header('Información general jugadores')
    res = json.loads(data)
    for idx, i in enumerate(res):
        res[idx] = json.loads(i)
    df = pd.DataFrame(res).astype(str)
    st.write(df)

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

def mainpage_requests():
    r_jugador_resume = requests.get(url='http://127.0.0.1:5000/api/jugador_resume').content

    jugador_resume_table(r_jugador_resume)

if __name__ == '__main__':
    settings_st()
    try:
        mainpage_requests()
    except requests.exceptions.ConnectionError:
        print('Connection refused')