import requests
import json
url = 'https://apis.datos.gob.ar/series/api/series?ids=168.1_T_CAMBIOR_D_0_0_26&limit=1000&sort=desc&format=json'

response = requests.get(url,timeout=1)

if response.status_code == 200:
    data = response.json()
else:
    print('Error en la solicitud:', response.status_code)
