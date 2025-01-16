"""
This script processes environmental data collected from IoT devices, categorizing the data into test and training datasets for further analysis and machine learning applications. Here's what the script does:

1. **Input Data Processing:**
   - Reads input data from standard input (`sys.stdin`), where each line contains information such as:
     - `dispositivo` (device ID)
     - `hora` (hour)
     - `minuto` (minute)
     - `temperatura` (temperature)
     - `latitude` and `longitude`
     - `numeroOcorrencias` (number of occurrences)

2. **Data Aggregation:**
   - Aggregates data for each device, calculating:
     - Average temperature, latitude, longitude, hour, and minute.
     - Minimum and maximum temperatures for each device.
   - Stores this aggregated information in dictionaries.

3. **Classification:**
   - Categorizes the average temperature into classes:
     - 'Frio' (Cold): < 10°C
     - 'Moderado' (Moderate): 10–20°C
     - 'Quente' (Warm): 20–25°C
     - 'Alerta' (Alert): > 25°C

4. **Dataset Categorization:**
   - Splits devices into test and training datasets based on the device ID:
     - If the first character of the device ID is numeric, the data goes into the test dataset (`dfTeste`).
     - Otherwise, it goes into the training dataset (`dfTreinamento`).

5. **CSV File Management:**
   - Deletes old CSV files (`teste.csv` and `treinamento.csv`) to avoid appending duplicate data.
   - Saves the test and training datasets to new CSV files:
     - `teste.csv`: Contains test data.
     - `treinamento.csv`: Contains training data.

6. **Output Structure:**
   - Each dataset includes:
     - Hour, minute, minimum and maximum temperatures, latitude, longitude, and temperature class.

This script is useful for preprocessing IoT data, enabling further analysis and machine learning model training.
"""


#! /usr/bin/env python
import os
import sys
import pandas as pd

dfTeste = pd.DataFrame([], columns = ['dispositivo','hora', 'minuto', 'temp_minima', 'temp_maxima', 'latitude', 'longitude', 'Classe'])
dfTreinamento = dfTeste

ocorrencia = {}
ocorrenciaHora = {}
ocorrenciaMinuto = {}
ocorrenciaTemperatura = {}
ocorrenciaTemperaturaMinima = {}
ocorrenciaTemperaturaMaxima = {}
ocorrenciaLatitude= {}
ocorrenciaLongitude= {}


for linha in sys.stdin:

    dispositivo, hora, minuto, temperatura, latitude, longitude, numeroOcorrencias = linha.split('\t')

    hora = int(hora)
    minuto = int(minuto)
    temperatura = float(temperatura)
    latitude = round(float(latitude),2)
    longitude = round(float(longitude),2)
    numeroOcorrencias = int(numeroOcorrencias)

    try:
        ocorrencia[dispositivo] = ocorrencia[dispositivo] + numeroOcorrencias
        ocorrenciaHora[dispositivo] = ocorrenciaHora[dispositivo] + hora
        ocorrenciaMinuto[dispositivo] = ocorrenciaMinuto[dispositivo] + minuto
        ocorrenciaTemperatura[dispositivo] = ocorrenciaTemperatura[dispositivo] + temperatura

        if ocorrenciaTemperaturaMinima[dispositivo] > temperatura:
            ocorrenciaTemperaturaMinima[dispositivo] = temperatura

        if ocorrenciaTemperaturaMaxima[dispositivo] < temperatura:
            ocorrenciaTemperaturaMaxima[dispositivo] = temperatura

        ocorrenciaLatitude[dispositivo] = ocorrenciaLatitude[dispositivo] + latitude
        ocorrenciaLongitude[dispositivo] = ocorrenciaLongitude[dispositivo] + longitude

    except:
        ocorrencia[dispositivo] = numeroOcorrencias
        ocorrenciaHora[dispositivo] = hora
        ocorrenciaMinuto[dispositivo] = minuto
        ocorrenciaTemperatura[dispositivo] = temperatura
        ocorrenciaTemperaturaMaxima[dispositivo] = temperatura
        ocorrenciaTemperaturaMinima[dispositivo] = temperatura
        ocorrenciaLatitude[dispositivo] = latitude
        ocorrenciaLongitude[dispositivo] = longitude



for dispositivo in ocorrencia.keys():

    temperaturaMedia = ocorrenciaTemperatura[dispositivo] / ocorrencia[dispositivo]
    hora = ocorrenciaHora[dispositivo] / ocorrencia[dispositivo]
    minuto = ocorrenciaMinuto[dispositivo] / ocorrencia[dispositivo]
    temp_minima = ocorrenciaTemperaturaMinima[dispositivo]
    temp_maxima = ocorrenciaTemperaturaMaxima[dispositivo]
    latitude = ocorrenciaLatitude[dispositivo] / ocorrencia[dispositivo]
    longitude = ocorrenciaLongitude[dispositivo] / ocorrencia[dispositivo]

    if temperaturaMedia < 10:
        classe = 'Frio'
    elif temperaturaMedia < 20:
        classe = 'Moderado'
    elif temperaturaMedia < 25:
        classe = 'Quente'
    else:
        classe = 'Alerta'

    base = 0.05
    latitude = round(base * round(float(latitude) / base), 2)
    longitude = round(base * round(float(longitude) / base), 2)

    #  Se for numerico entra na base de testes, se nao, na base treinamento

    try:
        testeNumerico = int(dispositivo[0])
        dfTeste = dfTeste.append({'dispositivo':dispositivo,'hora':hora, 'minuto':minuto, 
            'temp_minima': temp_minima, 'temp_maxima': temp_maxima, 
            'latitude':latitude, 'longitude':longitude, 'Classe':classe}, ignore_index=True)

    except:
        dfTreinamento = dfTreinamento.append({'dispositivo':dispositivo,'hora':hora, 'minuto':minuto, 
            'temp_minima': temp_minima, 'temp_maxima': temp_maxima, 
            'latitude':latitude, 'longitude':longitude, 'Classe':classe}, ignore_index=True)

# Apagar Arquivos Csv anteriores
try:
    os.remove("/root/om/PreProcessamento/teste.csv")
except:
    pass
try:
    os.remove("/root/om/PreProcessamento/treinamento.csv")
except:
    pass

dfTeste = dfTeste[['hora', 'minuto', 'temp_minima', 'temp_maxima', 'latitude', 'longitude', 'Classe']]
dfTreinamento = dfTreinamento[['hora', 'minuto', 'temp_minima', 'temp_maxima', 'latitude', 'longitude', 'Classe']]

dfTeste.to_csv(r"/root/om/PreProcessamento/teste.csv", index = False)
dfTreinamento.to_csv(r"/root/om/PreProcessamento/treinamento.csv", index = False)

## Criar base de testes
#count = 0
#csvresult = open("/root/om/PreProcessamento/teste.csv","a")
#while count < len(dfTeste):
#    hora = dfTeste['hora'][count]
#    minuto = dfTeste['minuto'][count]
#    temp_minima = dfTeste['temperaturaMinima'][count]
#    temp_maxima = dfTeste['temperaturaMaxima'][count]
#    latitude = dfTeste['latitude'][count]
#    longitude = dfTeste['longitude'][count]
#    classe = dfTeste['classe'][count]
#    
#    csvRow = '{},{},{},{},{},{},{}'.format(hora, minuto, temp_minima,
#                        temp_maxima, latitude, longitude, classe)
#
#    csvresult.write(csvRow + "\n")
#
#csvresult.close
#
## Criar base de Treinamento
#count = 0
#csvresult = open("/root/om/PreProcessamento/treinamento.csv","a")
#while count < len(dfTreinamento):
#    hora = dfTreinamento['hora'][count]
#    minuto = dfTreinamento['minuto'][count]
#    temp_minima = dfTreinamento['temperaturaMinima'][count]
#    temp_maxima = dfTreinamento['temperaturaMaxima'][count]
#    latitude = dfTreinamento['latitude'][count]
#    longitude = dfTreinamento['longitude'][count]
#    classe = dfTreinamento['classe'][count]
#    
#    csvRow = '{},{},{},{},{},{},{}'.format(hora, minuto, temp_minima,
#                        temp_maxima, latitude, longitude, classe)
#
#    csvresult.write(csvRow + "\n")
#
#csvresult.close
