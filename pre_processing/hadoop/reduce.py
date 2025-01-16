"""
This script processes key-value pairs from standard input (stdin), determining the highest temperature recorded for each unique device (key) and the associated location. Here’s what the script does:

1. **Initialization:**
   - Creates dictionaries:
     - `ocorrencia`: Stores the highest temperature for each device.
     - `ocorrenciaLocalizacao`: Stores the location corresponding to the highest temperature for each device.
   - Initializes a counter `total` to track the number of processed lines.

2. **Input Processing:**
   - Reads lines from `sys.stdin`.
   - Each line is expected to have three tab-separated (`\t`) values:
     - `chave` (key/device ID)
     - `temperatura` (temperature)
     - `localizacao` (location)
   - Increments the `total` counter for each line processed.

3. **Data Validation:**
   - Attempts to convert the `temperatura` to a floating-point number.
   - Skips invalid lines where `temperatura` cannot be converted.

4. **Temperature Comparison:**
   - Compares the current temperature (`temperatura`) with the stored value for the corresponding `chave` in `ocorrencia`:
     - If the current temperature is higher, updates the stored temperature and location.
     - If the `chave` does not exist in the dictionary, initializes it with the current temperature and location.

5. **Output Generation:**
   - Iterates through the keys in `ocorrencia`.
   - Prints the device ID (`chave`), highest temperature, and corresponding location in tab-separated format.

6. **Use Case:**
   - This script is useful for processing IoT data streams where you need to identify the maximum recorded temperature for each device and its location.

"""

#! /usr/bin/env python
import sys

ocorrencia = {}
ocorrenciaLocalizacao = {}
total = 0

for linha in sys.stdin:

    chave, temperatura, localizacao = linha.split('\t')
    total = total + 1
    
    try:
        temperatura = float(temperatura)
    except ValueError:
        continue
    
    try:
        if ocorrencia[chave] < temperatura:
            ocorrencia[chave] = temperatura
            ocorrenciaLocalizacao[chave] = localizacao
    except:
        ocorrencia[chave] = temperatura #  declara a chave
        ocorrenciaLocalizacao[chave] = localizacao

for dispositivo in ocorrencia.keys():
    print('{}\t{}\t{}'.format(dispositivo, ocorrencia[dispositivo], ocorrenciaLocalizacao[dispositivo]))
