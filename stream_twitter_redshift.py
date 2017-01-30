from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from nltk.tokenize import word_tokenize
from trending_words import preprocess
from nltk.corpus import stopwords

from datetime import datetime
from dateutil import parser
import sys
import psycopg2
import time
import ijson
import operator 
import json
import string
from collections import Counter
from collections import OrderedDict
import threading
#from impala.dbapi import connect


lista_data = []


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



class listener(StreamListener):
    
    def on_data(self, data):
        try:
            ##print(data)
            ##return True
            
            #cursor = conn.cursor()
            #print(data)
            #dataJson = json.load(data)

            ##print(json.dumps(dataJson, indent=4)) 
            ##return Truejson_load = json.loads(data)
            json_load = json.loads(data)
            
            if 'text' in json_load:
                texts = json_load['text']
            else:
                texts = ''
            #coded = texts.encode('utf-8')
            #s = str(coded)
            tweet = str(texts.encode('utf-8'))
            
            if 'created_at' in json_load:
                if json_load['created_at'] is None:
                    tweetDate = datetime.datetime.now().strftime("%a %b %d %H:%M:%S %z %Y")
                else:
                    tweetDate = str(json_load['created_at'])
            else:
                tweetDate = datetime.now().strftime("%a %b %d %H:%M:%S %z %Y")
                
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
                    userName = str(json_load['user']['screen_name'])
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

            ## UBICACIÓN DEL TUIT         
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
                    

            ## PAÍS
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

            
            
            ## insertamos la data
            ##cursor.execute("""INSERT INTO bigdatatwitter(codigo,fecha,hora,usuario,followers,mensaje,favoritos,retweets,country,ubicacion,latitud,longitud, candidato)
##VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s);""",[tweetId, fechaTuit, horaTuit, userName, followers, tweet, favoritos, retweets, pais,
##                                                            ubicacion, latitud,longitud, candidato])
##            conn.commit()
            
            data_collection = OrderedDict()
            data_collection['id'] = tweetId
            data_collection['fecha'] = fechaTuit
            data_collection['hora'] = horaTuit
            data_collection['usuario'] = userName
            data_collection['followers'] = followers
            data_collection['mensaje'] = tweet
            data_collection['favoritos'] = favoritos
            data_collection['retweets'] = retweets
            data_collection['country'] = pais
            data_collection['ubicacion'] =  ubicacion
            data_collection['latitud'] = latitud
            data_collection['longitud'] = longitud
            data_collection['candidato'] = candidato

            lista_data.append(data_collection)

            print(len(lista_data))
            return True
        
            #self.trending_words()
            #time.sleep(5)
            #cursor.close()
            return True
        except BaseException as e:
            print('failed ondata,' + str(e))
            
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
            #for comun in comunes:
                #val
                #query_insert = cursor2.execute("""INSERT INTO words(palabra,repeticiones) VALUES(%s,%s);""", [comun[0].decode().encode('utf-8'), int(comun[1])])
                #print(str(comun[0]))
                #print(int(comun[1]))
            
            cursor2.close()
            threading.Timer(5.0, trending_words).start()
        except BaseException as e:
            print('error,' + str(e))


def insertar_data():
    try:
        #cursor = conn.cursor()
        if len(lista_data) > 0:
            
            for item in lista_data:
                print(item)
                
             ## insertamos la data
           ## cursor.execute("""INSERT INTO bigdatatwitter(codigo,fecha,hora,usuario,followers,mensaje,favoritos,retweets,country,ubicacion,latitud,longitud, candidato)
    ##VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s);""",[tweetId, fechaTuit, horaTuit, userName, followers, tweet, favoritos, retweets, pais,
      ##                                                          ubicacion, latitud,longitud, candidato])
            
        #conn.commit()
        #cursor.close()
        # Reiniciamos el array
        #lista_data = []
        #print("palabras comunes")
        #print(len(lista_data))
        threading.Timer(5.0, insertar_data).start()
    except BaseException as e:
            print('error,' + str(e))
        
#trending_words()
insertar_data()

auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)
twitterStream = Stream(auth, listener())
twitterStream.filter(track=["trump"])



conn.close()


