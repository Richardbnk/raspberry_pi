"""
This script combines IoT and machine learning for real-time data classification and alerting:

1. MQTT Connection:
   - Connects to the HiveMQ public MQTT broker.
   - Subscribes to two topics:
     - 'main/OMIoT/team/valores_instantaneos': Receives instantaneous measurements.
     - 'main/OMIoT/team/valores_medios': Receives averaged measurements.

2. Machine Learning Model:
   - Uses a K-Nearest Neighbors (KNN) classifier with 4 neighbors.
   - Trains the model on a dataset ('treinamento.csv') with features like temperature and location.
   - Predicts the class (e.g., 'Alerta') for incoming data.

3. Real-Time Classification and Alerts:
   - Processes incoming MQTT messages.
   - If the classification result is 'Alerta', publishes an 'on' alert to 'main/OMIoT/team/alerta'.
   - Otherwise, publishes 'off'.

4. Data Logging:
   - Logs raw instantaneous and average data into respective CSV files for further analysis.

5. Long-Running Execution:
   - Runs for 15 days continuously, processing messages and publishing alerts as needed.
"""

#! /usr/bin/env python
import paho.mqtt.client as mqtt
import time
import pandas as pd

from sklearn.neighbors import KNeighborsClassifier
from sklearn import  model_selection
from sklearn.model_selection import train_test_split

client = mqtt.Client("clientId-fdsmdfeRNr") # clientId- add 10 caracteres aleatorios
client.connect("broker.hivemq.com", 1883)

clfa = KNeighborsClassifier(n_neighbors=4)

def treinarClassificadorKNN():

    global clfa 

    raw_data = '/root/om/PreProcessamento/treinamento.csv' 

    data = pd.read_csv(raw_data)

    x = data[['temp_minima','temp_maxima','latitude', 'longitude']]
    #x = data[['temp_minima','temp_maxima']]
    y = data['Classe']


    # EXEMPLO USANDO HOLDOUT
    # Holdout -> dividindo a base em treinamento (70%) e teste (30%), estratificada
    x_train, x_test, y_train, y_test = train_test_split(x,y,test_size=.5, random_state=42)

    # declara o classificador
    #clfa = KNeighborsClassifier(n_neighbors=4)

    # treina o classificador
    clfa = clfa.fit(x_train, y_train)

    # testa usando a base de testes
    #predicted=clfa.predict(x_test)



def classificarMedicao(columns):

    global clfa 

    hora = columns[0]
    minuto = columns[1]
    temp_minima = columns[2]
    temp_maxima = columns[3]
    latitude = columns[4]
    longitude = columns[5]

    baseTeste = pd.DataFrame(columns=['temp_minima', 'temp_maxima', 'latitude', 'longitude'])
    baseTeste = baseTeste.append({'temp_minima': temp_minima,
                                    'temp_maxima': temp_maxima,
                                    'latitude': latitude,
                                    'longitude': longitude
                                    }, ignore_index=True)
    
#    baseTeste = pd.DataFrame(columns=['temp_minima', 'temp_maxima'])
#    baseTeste = baseTeste.append({'temp_minima': temp_minima,
#                                    'temp_maxima': temp_maxima
#                                    }, ignore_index=True)


    predicted = clfa.predict(baseTeste)

    print('Classe: {}'.format(predicted[0]))

    # PUBLISH - Inserir linha na Cloud MQTT
    if predicted[0] == 'Alerta':
        client.publish("main/OMIoT/team/alerta", 'on')
    else:
        client.publish("main/OMIoT/team/alerta", 'off')


def on_message(client, userdata, msg):

    menssagem = msg.payload.decode()
    topico = msg.topic 

    if topico == 'main/OMIoT/team/valores_instantaneos':

        columns = menssagem.split(';')

        print("Instataneos importado:'{}'".format(menssagem))

        #Append
        with open('/root/om/instantaneos.csv','a') as fd:
            fd.write('{}\n'.format(menssagem))

    elif topico == 'main/OMIoT/team/valores_medios':
        columns = menssagem.split(';')

        classificarMedicao(columns)

        print("Medios importado:'{}'".format(menssagem))

        #Append
        with open('/root/om/medios.csv','a') as fd:
            fd.write('{}\n'.format(menssagem))

treinarClassificadorKNN()

client.on_message = on_message
client.loop_start()
client.subscribe("main/OMIoT/team/valores_instantaneos")
client.subscribe("main/OMIoT/team/valores_medios")

time.sleep(1 * 60 * 60 * 24 * 15) 
