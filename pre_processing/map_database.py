"""
This script processes IoT data from standard input (stdin) and prepares it for aggregation or further processing. It validates and formats data while normalizing latitude and longitude values. Here's a breakdown of its functionality:

1. **Input Parsing:**
   - Reads lines from `sys.stdin`, each expected to be semicolon-separated (`;`).
   - Extracts the following fields:
     - `dispositivo`: Device ID
     - `hora`: Hour
     - `minuto`: Minute
     - `ano`: Year
     - `mes`: Month
     - `dia`: Day
     - `temperatura`: Temperature
     - `latitude`: Latitude
     - `longitude`: Longitude

2. **Data Validation:**
   - Skips lines with any empty fields.
   - Ensures only complete records are processed.

3. **Location Normalization:**
   - Normalizes `latitude` and `longitude` values to a precision of 0.05 degrees.
   - This ensures consistency and reduces minor variations in location data.

4. **Output Formatting:**
   - Outputs the processed data in a tab-separated format (`\t`) with the following fields:
     - `dispositivo` (Device ID)
     - `hora` (Hour)
     - `minuto` (Minute)
     - `temperatura` (Temperature)
     - `latitude` (Normalized Latitude)
     - `longitude` (Normalized Longitude)
     - A constant value of `1` (likely for aggregation purposes)

5. **Example Use Case:**
   - This script is ideal for pre-processing IoT data to prepare it for aggregation or analysis. For instance, the normalized location data and timestamp can be used for calculating metrics or generating heatmaps.

6. **Example Input:**
```
device1;12;30;2025;01;15;23.5;-25.4966;-49.2619
device2;13;45;2025;01;15;24.0;-25.4970;-49.2620
```

7. **Example Output:**
```
device1\t12\t30\t23.5\t-25.5\t-49.25\t1
device2\t13\t45\t24.0\t-25.5\t-49.25\t1
```

**Note:** The output format uses Python 2-style string interpolation (`print '%s\t%s\t...'`), which should be updated to Python 3 (`print('{}\t{}\t...'.format(...))`) if necessary.
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

    # remove empty rows
    if dispositivo != '' and hora != '' and minuto != '' and ano != '' and mes != '' and \
        dia != '' and temperatura != '' and latitude != '' and longitude != '':

        base = 0.05

        latitude = round(base * round(float(latitude) / base), 2)
        longitude = round(base * round(float(longitude) / base), 2)

        print '%s\t%s\t%s\t%s\t%s\t%s\t%s' % (dispositivo, hora, minuto, temperatura, latitude, longitude, 1)
