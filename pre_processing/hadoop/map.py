"""
This script processes input data from standard input (stdin), typically used in a pipeline or batch processing system. Here's what the code does:

1. **Input Parsing:**
   - Reads lines of data from `sys.stdin`.
   - Each line is expected to be semicolon-separated (`;`) and contains:
     - `dispositivo` (device ID)
     - `hora` (hour)
     - `minuto` (minute)
     - `ano` (year)
     - `mes` (month)
     - `dia` (day)
     - `temperatura` (temperature)
     - `latitude`
     - `longitude`

2. **Data Validation:**
   - Ensures no line with empty fields is processed.
   - Skips lines where any of the key columns are empty.

3. **Location Normalization:**
   - Normalizes the latitude and longitude to a precision of 0.05 degrees for consistency.
   - This is achieved by rounding to the nearest multiple of 0.05.

4. **Output Generation:**
   - Outputs the processed data in a tab-separated format (`\t`), including:
     - `dispositivo` (device ID)
     - `temperatura` (temperature)
     - `latitude` (normalized)
     - `longitude` (normalized)

5. **Use Case:**
   - This script is useful for pre-processing IoT or geospatial data, ensuring that locations are standardized and incomplete data is excluded.

**Note:** The output format uses Python 2 style (`print '%s\t%s\t%s, %s'`), which should be updated to Python 3 if needed.
"""


#! /usr/bin/env python
import sys

for linha in sys.stdin:
    linha = linha.strip()

    columns = linha.split(';')

    dispositivo = columns[0]
    hora = columns[1]
    minuto = columns[2]
    ano = columns[3]
    mes = columns[4]
    dia = columns[5]
    temperatura = columns[6]
    latitude = columns[7]
    longitude = columns[8]

    #Nao inclui linhas com informacoes vazias
    if dispositivo != '' or hora != '' or minuto != '' or ano != '' or mes != '' or \
        dia != '' or temperatura != '' or latitude != '' or longitude != '':

        base = 0.05
        base * round(float(latitude) / base)

        latitude = round(base * round(float(latitude) / base), 2)
        longitude = round(base * round(float(longitude) / base), 2)

        print '%s\t%s\t%s, %s' % (dispositivo, temperatura, latitude, longitude)
