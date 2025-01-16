# The device ID has been set to match the Raspberry Pi's Wi-Fi MAC Address, 
# eliminating the need for using a hash code (MD5) and automatically adapting to each device.

import paho.mqtt.client as mqtt
from datetime import datetime
import random
import math
from time import sleep
import pandas as pd
import Adafruit_DHT as dht
import RPi.GPIO as gpio
   
gpio.setmode(gpio.BOARD)
gpio.setup(32, gpio.OUT)

latitude = -25.4966884
longitude = -49.2619725

dfValoresInstantaneos = pd.DataFrame([], columns = ['id', 'ano', 'mes', 'dia', 'hora', 'minuto', 'segundo', 'temperatura', 'latitude', 'longitude'])
dfValoresMedios = pd.DataFrame([], columns = ['hora', 'minuto', 'temperaturaMinima', 'temperaturaMaxima', 'latitude', 'longitude'])

horaAtual = 0
horaInicial = int(str(datetime.now())[11:13])

minutoInicial = 0

raioMaximo = 0.05

client = mqtt.Client("clientId-fdsmnMGRNr") # clientId- add 10 caracteres aleatorios
client.connect("broker.hivemq.com", 1883)

def on_message(client, userdata, msg):

    global gpio

    menssagem = msg.payload.decode()
    topico = msg.topic 

    if topico == 'PUCPR/OMIoT/EquipeBanak/alerta':
        if menssagem == 'on':
            print('ALERTA - Led Ligado')
            gpio.output(32, 1) #Ligando o pino 32
        else:
            print('ALERTA - Desligar Led')
            gpio.output(32, 0) #desligando o pino 32
    else:
        print("Alerta: '{}'   MSG:'{}'".format(msg.topic, msg.payload.decode()))


def getMAC(interface):
    # Retornar MAC Address da interface especificada
    try:
        str = open('/sys/class/net/{}/address'.format(interface)).read()
    except:
        str = "00:00:00:00:00:00"
    return str[0:17]

#Funcao para gerar o deslocamento maximo da localização em no maximo 0,05 graus
def gerarDeslocamentoMaximo(raioMaximo):
    x = random.uniform(0, raioMaximo)

    #Calcular deslocamento de Y com base no deslocamento de X (Regra de Pitagoras)
    y = math.sqrt((raioMaximo ** 2) - (x ** 2))
    print('X: {}   Y: {}   Raio: {}'.format(x, y, raioMaximo))

    #O deslocamento de X e Y pode ser positivo ou negativo considerando o deslocamento em um plano cartesiano
    x = x * random.choice((-1, 1))
    y = y * random.choice((-1, 1))

    return x, y



def gerarValoresInstantaneos():

    global minutoInicial
    global horaInicial
    global dfValoresInstantaneos
    global client

    #Carregar a base com Medias quando mudar de hora
    horaAtual = int(str(datetime.now())[11:13])
    if horaInicial != horaAtual:
        gerarValoresMedios()

    ano = int(str(datetime.now())[0:4])
    mes = int(str(datetime.now())[5:7])
    dia = int(str(datetime.now())[8:10])
    hora = int(str(datetime.now())[11:13])
    minuto = int(str(datetime.now())[14:16])
    segundo = int(str(datetime.now())[17:19])

    id_mac = getMAC('wlan0') # Mac Address do Wifi

    # Extrair temperatuda do DHT11
    umidade, temperatura = dht.read_retry(dht.DHT11, 4)
    temperatura = float(temperatura)

    #Carregar a primeira linha
    if len(dfValoresInstantaneos) == 0:
        dfValoresInstantaneos = dfValoresInstantaneos.append({'id':id_mac, 'ano':ano, 'mes':mes, 'dia':dia, 'hora':hora, 
                            'minuto':minuto, 'segundo':segundo, 'temperatura':temperatura, 
                            'latitude':latitude, 'longitude':longitude}, ignore_index=True)
        minutoInicial = minuto

    #Carregar o Array quando mudar de minuto
    minutoAtual = minuto
    if minutoInicial != minutoAtual and len(dfValoresInstantaneos) > 0:

        #Carregar linha apenas quando mudar de minuto
        dfValoresInstantaneos = dfValoresInstantaneos.append({'id':id_mac, 'ano':ano, 'mes':mes, 'dia':dia, 'hora':hora, 
                            'minuto':minuto, 'segundo':segundo, 'temperatura':temperatura, 
                            'latitude':latitude, 'longitude':longitude}, ignore_index=True)

        row = dfValoresInstantaneos.loc[len(dfValoresInstantaneos)-1]

        print('Carregando row Instantaneo no CSV: {}'.format(row))
        csvRow = '{};{};{};{};{};{};{};{};{};{}'.format(row['id'], row['ano'], row['mes'], row['dia'], row['hora'], row['minuto'], 
                                                        row['segundo'], row['temperatura'], row['latitude'], row['longitude'])

        # PUBLISH - Inserir linha de instantaneos na Cloud MQTT
        client.publish("PUCPR/OMIoT/EquipeBanak/valores_instantaneos", csvRow)

        csvresult = open("/home/pi/OficinaMaker/resultadoRemoto.csv","a")
        csvresult.write(csvRow + "\n")
        csvresult.close

    minutoInicial = minuto

    sleep(5)



def gerarValoresMedios():

    global horaInicial
    global client

    #gerar media das ultimas 60 medicoes do Array (Hora Media, Minuto Medio, TemperaturaMinima, TemperaturaMaxima, Latitude media, Longitude Media)
    numeroMedicoes = 60

    if len(dfValoresInstantaneos) >= numeroMedicoes:
        count = len(dfValoresInstantaneos) - numeroMedicoes
    else:
        count = 0

    horaMedia = 0
    minutoMedia = 0
    latitudeMedia = 0
    longitudeMedia = 0
    temperaturaMinima = ''
    temperaturaMaxima = ''

    countMedicoes = 0

    while count < len(dfValoresInstantaneos):
        horaMedia      = horaMedia      + dfValoresInstantaneos.loc[count]['hora']
        minutoMedia    = minutoMedia    + dfValoresInstantaneos.loc[count]['minuto']
        latitudeMedia  = float(latitudeMedia)  + float(dfValoresInstantaneos.loc[count]['latitude'])
        longitudeMedia = float(longitudeMedia) + float(dfValoresInstantaneos.loc[count]['longitude'])

        temperatura = float(dfValoresInstantaneos.loc[count]['temperatura'])
        
        if temperaturaMaxima == '' or temperaturaMaxima < temperatura:
            temperaturaMaxima = temperatura

        if temperaturaMinima == '' or temperaturaMinima > temperatura:
            temperaturaMinima = temperatura

        countMedicoes = countMedicoes + 1
        count = count + 1

    horaMedia = horaMedia / countMedicoes
    minutoMedia = minutoMedia / countMedicoes
    latitudeMedia = latitudeMedia / countMedicoes
    longitudeMedia = longitudeMedia / countMedicoes

    csvRow = '{};{};{};{};{};{}'.format(horaMedia, minutoMedia, temperaturaMinima, 
                                        temperaturaMaxima, latitudeMedia, longitudeMedia)

    print('Carregando Array Medio no CSV para o Hadoop: {}'.format(csvRow))

    # PUBLISH - Inserir linha de valores medios na Cloud MQTT
    client.publish("PUCPR/OMIoT/EquipeBanak/valores_medios", csvRow)

    csvresult = open("/home/pi/OficinaMaker/resultadoMedio.csv","a")
    csvresult.write(csvRow + "\n")
    csvresult.close

    horaInicial = int(str(datetime.now())[11:13])

    if horaInicial == 0:
        deslocamentoLatitude, deslocamentoLongitude = gerarDeslocamentoMaximo(raioMaximo)


def main():

    global client 

    client.on_message = on_message
    client.loop_start()
    client.subscribe("PUCPR/OMIoT/EquipeBanak/alerta")

    while True:
        gerarValoresInstantaneos()
    
    client.disconnect()

main()
