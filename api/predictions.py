import pandas as pd
import datetime
from octopy.logistic_model import LogisticModel
import re #regex
import streamlit as st

#get English premier league dataset
path_to_data = "data/prediction_data/"
data = pd.read_csv(path_to_data + 'epl.csv')

#Muestra los datos en formato grafico de barras y en tabla usando streamlit
def show_predictions():
    st.header('Predicción para victorias como locales')
    st.write('Se muestran los coeficientes predecidos para las victorias como local, valores positivos indican probabilidad de ganar en casa para equipos que juegan de local, mientras que para equipos que juegan de visitantes, los valores más negativos indican más probabilidades de ganar')
    res = get_predictions() 
    #res[0] -> almacena todos los valores desordenados
    #res[1] -> alamcena la lista de tuplas conlos resultados 
    #res[2] -> almacena la lista con los nombres de los equipos 

    df_chart_data = pd.DataFrame(res[1], index=res[2], columns=["Local Coeficient", "Away Coeficient"])
    #print(df_chart_data.iloc[0]["Local Coeficient"])
    st.bar_chart(df_chart_data)

    df = pd.DataFrame(res[1], index=[res[2]], columns=["Local Coeficient", "Away Coeficient"]).astype(str)
    st.write(df)

def get_predictions():
    #get the data in the train date range
    data.date = pd.DatetimeIndex(data.date)  
    #We use the results of the last 5 seasons to train our model that why datetime.datetime(2016,3,4) stands for
    data_train = data.loc[(data.date < datetime.datetime(2021,3,1)) & (data.date >= (datetime.datetime(2016,3,4)))]

    #train the model
    model = LogisticModel()
    model.fit(data_train.home,data_train.away,data_train.home_goals,data_train.away_goals)

    #get the table from the 
    teams_coef = model.get_coef()[['home wins']].sort_values('home wins',ascending=False) .round(2)
    teams_coef.index = [x[0] for x in teams_coef.index.values.ravel()]

    teams_names = teams_coef.index # recoge unicamente los valores de la primera columna ( nombres equipo )
    #print(teams_names)
    
    

    list_away= []
    i =0
    for t in teams_names:
        if t[0] == "a": # si la primera letra es a de away
            t = re.sub('away_', '', t) #Eliminamos el indicador que es local del nombre
            list_away.append((t,  -1*teams_coef.iloc[i][0])) # invertimos el numero para indicar que cuando estos equipos juegan fuera este es su coeficiente de victoria
        i+=1
    #print(list_away)   

    list_home= []
    j =0
    for t in teams_names:
        if t[0] == "h": # si la primera letra es h de home
            t = re.sub('home_', '', t) #Eliminamos el indicador que es local del nombre
            list_home.append((t,teams_coef.iloc[j][0]))
        j+=1
    #print(list_home)  

    list_away.sort() #Ordenamos las dos listas de igual manera para que los valores de las tuplas se correpondan al mismo equipo
    list_home.sort() #Ordenamos las dos listas de igual manera para que los valores de las tuplas se correpondan al mismo equipo
    teams_names_sorted = []
    for x in teams_names:
        if x[0] == "h": # si la primera letra es h de home
            x = re.sub('home_', '', x) #Eliminamos el indicador que es local del nombre
            teams_names_sorted.append(x)
    teams_names_sorted.sort()

    tuplas_final = []
    for a in range(len(list_home)):
        #guardamos en la lista de tuplas el coeficiente como locales junto con el coeficiente como visitante
        tuplas_final.append((list_home[a][1], list_away[a][1])) 
    
    
    return teams_coef, tuplas_final, teams_names_sorted

get_predictions()
