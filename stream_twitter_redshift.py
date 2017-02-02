
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from nltk.tokenize import word_tokenize
from trending_words import preprocess
from nltk.corpus import stopwords

from datetime import datetime
from dateutil import parser
import sys
import os
import psycopg2
import time
import ijson
import operator 
import json
import string
import pandas as pd
from collections import Counter
from collections import OrderedDict
import threading
import warnings
warnings.filterwarnings("ignore", category=UnicodeWarning)

import boto3

    
reload(sys)
sys.setdefaultencoding('UTF8')


trend = ""

def ingresar_palabra():
    global trend
    trend = raw_input("\tIngrese una palabra para filtrar los tuits: ")


while str(trend) == "":
    print("\tINGRESAR PALABRA...")
    ingresar_palabra()

print(datetime.now().strftime("%a %b %d %H:%M:%S %z %Y"))


AWS_ACCESS_KEY_ID = 'AKIAJDKACGYI7CMFVNWQ'
AWS_SECRET_ACCESS_KEY = '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'

client = boto3.client('s3',
    # Hard coded strings as credentials, not recommended.
    aws_access_key_id='AKIAJDKACGYI7CMFVNWQ',
    aws_secret_access_key='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'
)

bucket_name = "datatwitter"




lista_data_procesada = []
lista_data_alt = []

file_name = ""

## Variable global del Thread
t = ""
d = ""

## variable contador general, nos ayudara a saber en que posicion se quedo el arreglo para poder eliminarl los primeros 'cont' elementos del array
cont = -1
contB = -1

ckey = 'gHc857rgGXVeXIOaPrw6JrHrt'
csecret = 'Ehug0Ogf3Y5c2CrDlHt3sZOPmu55AMLY3TFxzcUhrfISwyg0vx'
atoken = '360992070-uws1Ozanon4JsFLyklIFCspiCHonmlZfpyXUnEjv'
asecret = 'xKzVhAwgGJ4Z1Vq2h2njEhjzEUdxQwGU3byBrh0Yxl5fc'


config = { 'dbname': 'datamfdl',
           'user':'marco',
           'pwd':'Mfdl9124',
           'host':'web2016.cl3d0wulavec.us-west-2.redshift.amazonaws.com',
           'port':'5439'
           }

def create_conn(*args,**kwargs):
    config = kwargs['config']
    try:
        con=psycopg2.connect(dbname=config['dbname'], host=config['host'],
        port=config['port'], user=config['user'],password=config['pwd'])
        return con
    except Exception as err:
        print(err)
        
conn = create_conn(config=config)

cursor = None

class listener(StreamListener):
    
    def on_data(self, data):
        try:
            with open('tweets.txt', 'a') as f:
                f.write(data)
            lista_data_alt.append(data)
            
            return True
        except BaseException as e:
            print('failed ondata,' + str(e))
            ## Procesamos toda la data restante en el array
            insertar_data(True)                    
            time.sleep(5)
    
    def on_error(self, status):
        print(status)
    
        
        
        
def trending_words():       
        try:
            #print("hola")
            cursor2 = conn.cursor()
            query = """SELECT mensaje FROM bigdatatwitter;"""
            cursor2.execute(query)
            rows = cursor2.fetchall()

            count_all = Counter()

            # Stop words
            punctuation = list(string.punctuation)
            stop = stopwords.words('english') + punctuation + ['rt', 'via', 'RT']
            
            for row in rows:
                #print(row[0])
                # Creamos una lista de los terminos + frecuentes
                #terms_all = [term for term in preprocess(row[0])]
                #terms_stop = [term for term in preprocess(row[0]) if term not in stop]
                terms_hash = [term for term in preprocess(row[0]) if term.startswith('#')]
                # Actualizamos el contador
                count_all.update(terms_hash)

            #print(count_all.most_common(500))

            comunes = count_all.most_common(10)
            for comun in comunes:
                #val
                query_insert = cursor2.execute("""INSERT INTO words(palabra,repeticiones) VALUES(%s,%s);""", [comun[0].decode().encode('utf-8'), int(comun[1])])
                #print(str(comun[0]))
                #print(int(comun[1]))
            
            cursor2.close()
            threading.Timer(10.0, trending_words).start()
        except BaseException as e:
            print('error,' + str(e))


def insertar_data(all=False):
    try:
        print(threading.currentThread().getName())
        i = 0
        n = 1000
        lista = lista_data_alt
        longitud = len(lista)
        global cont
        global t
       

        data_procesada = []

        file_name = "data_twitter_" + str(time.time()) + ".csv"
        
        cursor = conn.cursor()
        if isinstance(t, threading.Thread):
            print("Es instancia de un Thread")
##            if t.isAlive():
##                print("Thread " + threading.currentThread().getName() + " is alive")
##                print("Esperamos a que termine el thread...")
##            else:
            print("Thread " + threading.currentThread().getName() + " is NOT alive")

            if longitud > 0:
                print("hay elementos")
                ## Primera iteracion, no eliminamos los elementos porque no hemos isertado nada
                if cont < 0:
                    print("Primera Iteracion, no se procesa nada")
                else:
                    print("Otra Iteracion, se eliminan los primeros " + str(cont) + " elementos")
                    ## Eliminamos los primeros cont elementos del array, puesto que ya se insertaron
                    if longitud >= cont:
                        del lista_data_alt[:cont]
                    
                ## Reiniciamos el contador
                cont = 0
                if all is True:
                    ## Ha habido una exception, entonces debemos de procesar todos los elementos del array
                    for item in lista:
                        data_procesada = procesar_data_tuits(item)
                        lista_data_procesada.append(data_procesada)
                        cont = cont + 1
                    
                else:
                    ## Nos aseguramos de iterar solo los elementos del array
                    for item in lista:
                        if cont <= n:
                            data_procesada.append(procesar_data_tuits(item))
                        else:
                            break                               
                    
                        #data_procesada = procesar_data_tuits(item)
                        #lista_data_procesada.append(data_procesada)
                        cont = cont + 1
                        
                    data_frame = pd.DataFrame(data_procesada)
                    data_frame.to_csv(file_name, header=False, encoding='utf-8', sep='|', float_format='%.7f', index=False)
                    insertar_en_bd(file_name)
            
            else:
                print("No hay elementos en el array")
                
        else:
            print("No es instancia de Thread")
        
        
        t = threading.Timer(5.0, insertar_data)
        ##t.daemon = True
        t.start()    
        
    except BaseException as e:
            print('error al procesar,' + str(e))
            

def insertar_en_bd(file_name_param):
    try:
                       
        if(file_name_param != "" and file_name_param is not None):
            print("Uploading " + file_name_param + " ...")
            time_start = time.time()
            client.upload_file(file_name_param,bucket_name, file_name_param)
            time_end = time.time()
            print("Data uploaded correctly")    
            print "This took %.3f seconds" % (time_end - time_start)

            time.sleep(3)
            
            cursor = conn.cursor()
            
            query = """copy bigdata from 's3://datatwitter/""" + file_name_param + """' credentials 'aws_iam_role=arn:aws:iam::388344987295:role/marco' delimiter '|' region 'us-west-2';"""
            cursor.execute(query)

            conn.commit()
            cursor.close()

            if os.path.exists(file_name_param):
                os.remove(file_name_param)
            else:
                print("Sorry, I can not remove %s file." % file_name_param)
                        
        else:
            print("File name incorrect")

       
    except BaseException as e:
        cursor.close()
        print('error al subir archivo, ' + str(e))


def procesar_data_tuits(data):

    json_load = json.loads(data)
            
    if 'text' in json_load:
        texts = json_load['text']
    else:
        texts = ''
    #coded = texts.encode('utf-8')
    #s = str(coded)
    tweet = texts.encode("utf-8")
    #tweet = unicode(tweet, errors='ignore')
    tweetDate = ""
    try:
        if 'created_at' in json_load:
            if json_load['created_at'] is None:
                tweetDate = datetime.now().strftime("%a %b %d %H:%M:%S %z %Y")
            else:
                tweetDate = str(json_load['created_at'])
        else:
            tweetDate = datetime.now().strftime("%a %b %d %H:%M:%S %z %Y")

        
    except BaseException as e:
        print('datetime exception')
        tweetDate = datetime.now().strftime("%a %b %d %H:%M:%S %z %Y")
        return True
        
    parsed_date = parser.parse(tweetDate)
    # Obtenemos la fecha y la hora del tuit por separado
    fechaTuit = parsed_date.strftime("%Y-%m-%d")
    horaTuit = parsed_date.strftime("%H:%M:%S")

    ## ID DEL TUIT
    if 'id' in json_load:
        tweetId = str(json_load['id'])
    else:
        tweetId = ""
    #print(tweetId)
    
    ## USUARIO DEL TUIT
    if 'user' not in json_load:
        userName = ""
        followers = 0
        
    else:
        ## NOMBRE DEL USUARIO
        if 'screen_name' in json_load['user']:
            userName = str(json_load['user']['screen_name'].encode('utf-8'))
        else:
            userName = ""
            
        ## FOLLOWERS DEL USUARIO
        if 'followers_count' in json_load['user']:
            followers = int(json_load['user']['followers_count'])
        else:
            followers = 0
    
   
    # FAVORITOS DEL TUIT
    if 'favorite_count' not in json_load:
        favoritos = 0
    else:
        favoritos = int(json_load['favorite_count'])

    
    ## RETUITS
    if 'retweet_count' not in json_load:
        retweets = 0
    else:
        retweets = int(json_load['retweet_count'])

    ## UBICACION DEL TUIT         
    latitud = 0.0000
    longitud = 0.0000

    if 'geo' not in json_load:
        latitud = 0.0000
        longitud = 0.0000
    else:
        if json_load['geo'] is None:
            latitud = 0.0000
            longitud = 0.0000                    
        else:
            latitud = float(json_load['geo']['coordinates'][1])
            longitud = float(json_load['geo']['coordinates'][0])
            

    ## PAIS
    if 'place' in json_load:
        location = json_load['place']
    else:
        location = None

    if location is None:
        pais = ''
        ubicacion = ''
    else:
        pais = str(json_load['place']['country'])
        ubicacion = str(json_load['place']['full_name'])

        
    candidato = 0

    ## Eliminamos los espacios del texto del tuit
    texto_procesado = tweet.replace('\n', ' ').replace('\r', '').replace('|', ' ')
    
    data_collection = OrderedDict()
    data_collection['id'] = tweetId
    data_collection['fecha'] = fechaTuit
    data_collection['hora'] = horaTuit
    data_collection['usuario'] = userName
    data_collection['followers'] = int(followers)
    data_collection['mensaje'] = texto_procesado
    data_collection['favoritos'] = int(favoritos)
    data_collection['retweets'] = int(retweets)
    data_collection['country'] = pais
    data_collection['ubicacion'] =  ubicacion
    data_collection['latitud'] = latitud
    data_collection['longitud'] = longitud

    return data_collection



    
trending_words()

insertar_data()


auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)
twitterStream = Stream(auth, listener())
twitterStream.filter(track=[trend])



conn.close()


