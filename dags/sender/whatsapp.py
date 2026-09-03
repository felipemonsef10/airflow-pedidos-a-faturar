from airflow.models import Variable
from pathlib import Path
import os
import requests
from requests.exceptions import HTTPError, RequestException


FOLDER_PDFS = Path('pdfs')

ID_INSTANCE = Variable.get("idInstance")
API_TOKEN_INSTANCE = Variable.get("apiTokenInstance")
CHAT_ID = '553484355709@c.us'

url = "https://7103.media.greenapi.com/waInstance{}/sendFileByUpload/{}"


def fluxo_envio_relatorios():
    for arquivo in os.listdir(FOLDER_PDFS):
        path = str(FOLDER_PDFS / arquivo)
        enviar_mensagem(ID_INSTANCE, API_TOKEN_INSTANCE, CHAT_ID, path)


def enviar_mensagem(idInstance, apiTokenInstance, chatId, path):
    url_ = url.format(idInstance, apiTokenInstance)

    payload = {
        'chatId': chatId
    }
    
    filename = path.split('/')[-1]
    files = [
        ('file', (filename, open(path,'rb'),'application/pdf'))
    ]
    headers= {}

    try:
        response = requests.post(url_, data=payload, files=files)
        
        response.raise_for_status()
    
        print(response.text.encode('utf8'))
    
    except HTTPError as http_err:
        print(f"HTTP error: {http_err}")
        print(f"Status code: {http_err.response.status_code}")
    except RequestException as req_err:
        print(f"Network error: {req_err}")
    


# from whatsapp_api_client_python import API
# greenAPI = API.GreenAPI(ID_INSTANCE, API_TOKEN_INSTANCE)

# def enviar_mensagem(chatId, path):
#     filename = path.split('/')[-1]
    
#     response = greenAPI.sending.sendFileByUpload(
#         chatId,
#         path,
#         filename,
#     )
    
#     print(response.data)