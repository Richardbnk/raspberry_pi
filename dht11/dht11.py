# for Humidity and temperature

#! /usr/bin/env python
import Adafruit_DHT as dht

umidade, temperatura = dht.read_retry(dht.DHT11, 4)

print('Humidity: {}'.format(umidade))
print('Temperature : {}'.format(temperatura))
