#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import csv
import sys
import botometer
import numpy
#import GetOldTweets3 as got
#import snap
import time
from datetime import datetime, timedelta
from operator import add
from textblob import TextBlob
from instaloader import Instaloader, Profile
import instaloader
import json
import re


def main():
    scrapping_header = 'ID,ConvID,Context,Disclosure Type,Has Parent Tweet,Has Cards,Date,Language,Permalink,is part of conversation?,Author ID,Author Name,is Author verified,Text,Replies,Retweets,Favorites,Mentions,is Reply?,is Retweet?,Reply to User ID,Reply To User Name,Quoted Tweet ID, Quoted Tweet Conv. ID,Quoted Tweet Type,Quoted Tweets User ID,Quoted Tweet User'
    nombres_proyectos = [#"Argentina 2015", "Argentina 2019",
                         #"Brasil 2014",
                         #"Brasil 2018",
                         "Colombia 2018",
                         #"Ecuador 2013", "Ecuador 2017",
                         #"Francia 2012", "Francia 2017",
                         #"India 2014", "India 2019", "Kenya 2017",
                         #"Nigeria 2011", "Nigeria 2015",
                         #"Sudáfrica 2019"
                         ]
    #[fecha_primera_vuelta, fecha_segunda_vuelta, candidato_1, candidato_2, candidato_3]
    datos_proyectos = {"Argentina 2015": [datetime(day=25, month=10, year=2015),datetime(day=22, month=11, year=2015),'mauriciomacri','danielscioli','SergioMassa'],
                       "Argentina 2019": [datetime(day=27, month=10, year=2019),datetime(day=27, month=10, year=2019),'alferdez','mauriciomacri','RLavagna'],
                       "Brasil 2014": [datetime(day=5, month=10, year=2014),datetime(day=26, month=10, year=2014),'dilmabr','AecioNeves','MarinaSilva'],
                       "Brasil 2018": [datetime(day=7, month=10, year=2018),datetime(day=28, month=10, year=2018),'jairbolsonaro','Haddad_Fernando','cirogomes'],
                       "Colombia 2014": [datetime(day=25, month=5, year=2014), datetime(day=15, month=6, year=2014),'JuanManSantos', 'OIZuluaga', 'mluciaramirez'],
                       "Colombia 2018": [datetime(day=27, month=5, year=2018), datetime(day=17, month=6, year=2018),'IvanDuque', 'petrogustavo', 'sergio_fajardo'],
                       "Ecuador 2013": [datetime(day=17, month=2, year=2013),datetime(day=17, month=2, year=2013),'MashiRafael','LassoGuillermo','LucioGutierrez3'],
                       "Ecuador 2017": [datetime(day=19, month=2, year=2017),datetime(day=2, month=4, year=2017),'Lenin','LassoGuillermo','CynthiaViteri6'],
                       "Francia 2012": [datetime(day=22, month=4, year=2012),datetime(day=6, month=5, year=2012),'fhollande','NicolasSarkozy','MLP_officiel'],
                       "Francia 2017": [datetime(day=23, month=4, year=2017),datetime(day=7, month=5, year=2017),'EmmanuelMacron','MLP_officiel','FrancoisFillon'],
                       "India 2014": [datetime(day=16, month=5, year=2014),datetime(day=16, month=5, year=2014),'narendramodi','RahulGandhi',''],
                       "India 2019": [datetime(day=19, month=5, year=2019),datetime(day=19, month=5, year=2019),'narendramodi','RahulGandhi',''],
                       "Kenya 2017": [datetime(day=8, month=8, year=2017),datetime(day=26, month=10, year=2017),'kenyatta','RailaOdinga',''],
                       "Nigeria 2011": [datetime(day=9, month=4, year=2011),datetime(day=16, month=4, year=2011),'GEJonathan','MBuhari','NuhuRibadu'],
                       "Nigeria 2015": [datetime(day=29, month=3, year=2015),datetime(day=29, month=3, year=2015),'MBuhari','GEJonathan',''],
                       "Sudáfrica 2019": [datetime(day=8, month=5, year=2019),datetime(day=8, month=5, year=2019),'CyrilRamaphosa','MmusiMaimane','Julius_S_Malema'],
                       "Alcaldia": [datetime(day=13, month=7, year=2020),datetime(day=10, month=8, year=2020),'parque','restaurante','ciclovía']}
    #get_tweets(datos_proyectos["Colombia 2014"])
    #pegue_csv(scrapping_header, "Parque-Restaurante-Ciclovía", "Parque-Restaurante-Ciclovía")
    #get_tweets(datos_proyectos["Alcaldia"])
    #to_sentiment("BACA 0410",["BogotáACieloAbierto","cielo abierto","restaurante","bogotacieloabierto","bogotaacieloabierto"])
    for nombre_proyecto in nombres_proyectos:
        carpeta = nombre_proyecto
        #pegue_csv(scrapping_header, carpeta, nombre_proyecto)
        result = crear_grafo(nombre_proyecto + '.csv', datos_proyectos, nombre_proyecto, with_hashtags=False)
        #crear_long_conexiones(users=result['users'], edges=result['edges'], nombre_proyecto=nombre_proyecto)


        # -150 a -121
        result = crear_grafo(nombre_proyecto + '.csv', datos_proyectos, nombre_proyecto, with_hashtags=False,
                             days_filter_lower=-151, days_filter_upper=-121)
        #crear_matriz_conexiones(users=result['users'], edges=result['edges'], nombre_proyecto=nombre_proyecto,
        #                        days_filter_lower=-151, days_filter_upper=-121)
        # -120 a -91
        result = crear_grafo(nombre_proyecto + '.csv', datos_proyectos, nombre_proyecto, with_hashtags=False,
                             days_filter_lower=-120, days_filter_upper=-91)
        #crear_matriz_conexiones(users=result['users'], edges=result['edges'], nombre_proyecto=nombre_proyecto,
        #                        days_filter_lower=-120, days_filter_upper=-91)
        #-90 a -61
        result = crear_grafo(nombre_proyecto + '.csv', datos_proyectos, nombre_proyecto, with_hashtags=False,
                             days_filter_lower=-90, days_filter_upper=-61)
        #crear_matriz_conexiones(users=result['users'], edges=result['edges'], nombre_proyecto=nombre_proyecto,
        #                        days_filter_lower=-90, days_filter_upper=-61)
        # -60 a -31
        result = crear_grafo(nombre_proyecto + '.csv', datos_proyectos, nombre_proyecto, with_hashtags=False,
                             days_filter_lower=-60, days_filter_upper=-31)
        #crear_matriz_conexiones(users=result['users'], edges=result['edges'], nombre_proyecto=nombre_proyecto,
        #                        days_filter_lower=-60, days_filter_upper=-31)
        # -30 a 0
        result = crear_grafo(nombre_proyecto + '.csv', datos_proyectos, nombre_proyecto, with_hashtags=False,
                             days_filter_lower=-30, days_filter_upper=0)
        #crear_matriz_conexiones(users=result['users'], edges=result['edges'], nombre_proyecto=nombre_proyecto,
        #                        days_filter_lower=-30, days_filter_upper=0)

    #to_botometer("Botometer", "tweetcount-full.csv", "tweetcount-full")

#Para obtener los tweets
def get_tweets(datos_proyecto):
    tweetCriteria = got.manager.TweetCriteria()\
        .setQuerySearch('('+datos_proyecto[2]+' OR '+datos_proyecto[3]+' OR '+datos_proyecto[4]+')')\
        .setSince((datos_proyecto[0]-timedelta(150)).strftime('%Y-%m-%d'))\
        .setUntil((datos_proyecto[0] + timedelta(150)).strftime('%Y-%m-%d'))
    tweets = got.manager.TweetManager.getTweets(tweetCriteria)
    print(tweets)
    print(len(tweets))


#Para pegar los archivos provenientes del OrgNeatUI
def pegue_csv(scrapping_header, carpeta, archivo_salida):
    csv_header = scrapping_header
    csv_out = archivo_salida + '.csv'

    csv_dir = os.getcwd()

    dir_tree = os.walk(os.path.join(csv_dir, carpeta))
    print('--------1-----------------------')
    print(os.path.join(csv_dir, carpeta))

    filenames = []
    for dirpath, dirnames, f in dir_tree:
        filenames.extend(f)

    csv_list = []
    for file in filenames:
        if file.endswith('.csv'):
            csv_list.append(file)

    csv_merge = open(csv_out, 'w', encoding="utf8")
    csv_merge.write(csv_header)
    csv_merge.write("\n")

    for file in csv_list:
        wait_list = []
        csv_in = open(os.path.join(carpeta, file), encoding="utf8")
        print (os.path.join(carpeta, file))
        for line in csv_in:
            line = str(line)
            if csv_header not in line:
                if line.count(',')>=scrapping_header.count(','):
                    if len(wait_list)>0:
                        wait_list.append('\n')
                        csv_merge.write(' '.join(wait_list))
                        wait_list = []
                wait_list.append(line.rstrip())
        csv_merge.write(' '.join(wait_list))
        csv_in.close()
    csv_merge.close()
    #print('Verify consolidated CSV file : ' + csv_out)

#Crea los datos para el análisis
def crear_grafo(archivo_tweets, datos_proyectos, nombre_salida, with_hashtags=True, days_filter_lower=None, days_filter_upper=None):
    users = []
    user_candidatos = []
    user_candidatos_rt = []
    user_aparicion = []
    edges = []
    hashtags = {}
    #G1 = snap.TNGraph.New()
    fecha_pv = datos_proyectos[nombre_salida][0]
    fecha_sv = datos_proyectos[nombre_salida][1]
    candidato_1 = str.lower(datos_proyectos[nombre_salida][2])
    candidato_2 = str.lower(datos_proyectos[nombre_salida][3])
    candidato_3 = str.lower(datos_proyectos[nombre_salida][4])

    with open(archivo_tweets, 'r', encoding='utf8', newline='') as File:
        reader = csv.DictReader(File)
        for tweet in reader:
            if 'Mentions' not in tweet.keys():
                sp = tweet['Text'].split(' ')
                ment = ''
                for s in sp:
                    if '@' in s:
                        s = s.replace("@",'')
                        if len(ment) == 0:
                            ment = s
                        else:
                            ment = ment + ' ' + s
                tweet['Mentions'] = ment
            if tweet['Permalink'] and tweet['Permalink'].count('/')>0 and tweet['Mentions']: #Elimina tweets sin id de usuario
                #Limpieza de tweets
                tweet['Text'] = tweet['Text'].replace("http", " http").replace("pic.twitter", " pic.twitter").replace("&quot;", " ").replace(";", " ").replace("#", " #").replace("@", " @")
                tweet['Text'] = str.lower(tweet['Text'])
                #Conseguir la fecha
                if 'T' in tweet['Date']:
                    tweet['Date'] = re.sub('.[0-9]*Z', '', tweet['Date'].replace("T", ' ').replace("-", "/"))
                    fecha = tweet['Date'].split(' ')
                    ymd = fecha[0].split('/')
                    hm = fecha[1].split(':')
                    fecha_dt = datetime(year=int(ymd[0]), month=int(ymd[1]), day=int(ymd[2]), hour=int(hm[0]),
                                        minute=int(hm[1]))
                else:
                    fecha = tweet['Date'].split(' ')
                    ymd = fecha[0].split('/')
                    hm = fecha[1].split(':')
                    fecha_dt = datetime(day=int(ymd[2]), month=int(ymd[1]), year=int(ymd[0]), hour=int(hm[0]), minute=int(hm[1]))
                delta_pv = (fecha_dt - fecha_pv).days
                delta_sv = (fecha_dt - fecha_sv).days
                #Booleans para conocer el(los) candidato(s) a los que menciona el tweet
                c1 = candidato_1 in tweet['Text'] and candidato_1 is not ''
                c2 = candidato_2 in tweet['Text'] and candidato_2 is not ''
                c3 = candidato_3 in tweet['Text'] and candidato_3 is not ''
                # Menciona a alguno de los candidatos y está dentro de los 150 días previos o posteriores a la elección
                if (c1 or c2 or c3) and delta_pv >= -150:
                    cs = [0,0,0]
                    crt = [0,0,0]
                    if c1 :
                        cs[0] = 1
                    if c2:
                        cs[1] = 1
                    if c3:
                        cs[2] = 1
                    if tweet['is Retweet?']:
                        crt = cs


                    #user = "@" + tweet['username'] #Ahora el username está en el permalink
                    user = '@' + tweet['Permalink'].split('/')[1]
                    print (user + ' ' + str(len(users)))
                    if not user in users and user is not '@':
                        #G1.AddNode(len(users))
                        users.append(user)
                        user_candidatos.append([0, 0, 0])
                        user_candidatos_rt.append([0, 0, 0])
                        user_aparicion.append(delta_pv)
                    # Filtro de usuarios
                    if (days_filter_lower is None and days_filter_upper is None) or (days_filter_lower <= delta_pv <= days_filter_upper):
                        user_candidatos[users.index(user)] = list(map(add, user_candidatos[users.index(user)], cs))
                        user_candidatos_rt[users.index(user)] = list(map(add, user_candidatos_rt[users.index(user)], crt))

                    if user_aparicion[users.index(user)] > delta_pv:
                        user_aparicion[users.index(user)] = delta_pv

                    #Mencionados
                    mentions = tweet['Mentions'].split(' ')
                    for m in mentions:
                        if m is not '':
                            m = '@' + m
                            if m not in users:
                                #G1.AddNode(len(users))
                                users.append(m)
                                user_candidatos.append([0, 0, 0])
                                user_candidatos_rt.append([0, 0, 0])
                                user_aparicion.append(delta_pv)
                            #G1.AddEdge(users.index(user), users.index(m))

                            if user_aparicion[users.index(m)] > delta_pv:
                                user_aparicion[users.index(m)] = delta_pv
                            # Filtro de menciones
                            if (days_filter_lower is None and days_filter_upper is None) or (days_filter_lower <= delta_pv <= days_filter_upper):
                                if m == '@'+tweet['Reply To User Name']: #Determina si es una réplica al usuario
                                    edges.append([users.index(user), users.index(m), fecha_dt, tweet['Text'], tweet['Retweets'],
                                         tweet['Favorites'], tweet['is Retweet?'], 'True', "mention", delta_pv, delta_sv, c1, c2 ,c3])
                                else:
                                    edges.append([users.index(user), users.index(m), fecha_dt, tweet['Text'], tweet['Retweets'],
                                                  tweet['Favorites'], tweet['is Retweet?'], 'False', "mention", delta_pv, delta_sv, c1, c2 ,c3])

                    #Hashtags
                    text_hts = tweet['Text'].split(' ')
                    hts = []
                    if with_hashtags: #Filtro de hashtags
                        for w in text_hts:
                            if w.startswith('#') and len(w)>=2:
                                hts.append(w)

                        if len(hts) > 1 and not hts[0] == '':
                            for h in hts:
                                if not h in hashtags:
                                    hashtags[h] = [user]
                                    #G1.AddNode(len(users))
                                    users.append(h)
                                    user_candidatos.append([0, 0, 0])
                                    user_candidatos_rt.append([0, 0, 0])
                                    user_aparicion.append(delta_pv)
                                else:
                                    hashtags[h].append(user)

                                if user_aparicion[users.index(h)] > delta_pv:
                                    user_aparicion[users.index(h)] = delta_pv
                                # Filtro de hashtags (user)
                                if (days_filter_lower is None and days_filter_upper is None) or (days_filter_lower <= delta_pv <= days_filter_upper):
                                    user_candidatos[users.index(h)] = list(map(add, user_candidatos[users.index(h)], cs))
                                    user_candidatos_rt[users.index(h)] = list(map(add, user_candidatos_rt[users.index(h)], crt))

                                #G1.AddEdge(users.index(user), users.index(h))
                                # Filtro de hashtags (edges)
                                if (days_filter_lower is None and days_filter_upper is None) or (days_filter_lower <= delta_pv <= days_filter_upper):
                                    edges.append([users.index(user), users.index(h), fecha_dt, tweet['Text'], tweet['Retweets'],
                                              tweet['Favorites'], tweet['is Retweet?'], 'NA', "hashtag", delta_pv, delta_sv, c1, c2 ,c3])

        print (len(edges))

    print ("-------------")
    filter_str = '' if days_filter_lower is None and days_filter_upper is None else 'filter('+str(days_filter_lower)+' '+str(days_filter_upper)+ ') '
    with open('Edgelist ' + filter_str + nombre_salida + '.csv', 'w', encoding="utf8", newline='') as File:
        writer = csv.writer(File, delimiter=';')
        writer.writerow(['source', 'target', 'fecha', 'text', 'retweets', 'favorites', 'is_retweet', 'is_reply', 'tipo',
                         'dias_eleccion', 'dias_segunda', 'candidato_1', 'candidato_2', 'candidato_3'])
        for e in edges:
            print(e)
            writer.writerow(e)

    print ("-------------")
    with open('Nodes ' + filter_str + nombre_salida + '.csv', 'w', encoding="utf8", newline='') as File:
        writer = csv.writer(File, delimiter=';')
        writer.writerow(['id', 'username', 'primer_tweet', 'menciones_c1', 'menciones_c2', 'menciones_c3',
                         'rt_c1', 'rt_c2', 'rt_c3', 'tipo'])
        users_print = []
        for i, screen_name in enumerate(users):
            if screen_name.startswith('@'):
                print(screen_name + " " + str(i))
                up = [i, screen_name, str(user_aparicion[i]),
                                 user_candidatos[i][0], user_candidatos[i][1],
                                 user_candidatos[i][2], user_candidatos_rt[i][0],
                                 user_candidatos_rt[i][1], user_candidatos_rt[i][2], "user"]
                writer.writerow(up)
                users_print.append(up)
            else:
                print(screen_name + " " + str(i))
                writer.writerow([i, screen_name, str(user_aparicion[i]),
                                 user_candidatos[i][0], user_candidatos[i][1],
                                 user_candidatos[i][2], user_candidatos_rt[i][0],
                                 user_candidatos_rt[i][1], user_candidatos_rt[i][2], "hashtag"])
    #return G1, hashtags
    return {'users': users_print, 'edges': edges, 'hashtags': hashtags}


# noinspection PyBroadException
def to_botometer(carpeta, nodelist, nombre_salida):
    # Botometer initialization
    rapidapi_key = "6e87fc4ad6msh16f38ddda67d8efp133e91jsn6df53b6f88db"  # now it's called rapidapi key
    twitter_app_auth = {
        'consumer_key': 'IFfp1WgFrDraT0OMYMNtsyLns',
        'consumer_secret': 'WJ3Go4nu1WtteR5y08tG7NpTo6JvxhnGMnWNA4jdxOooo6m8GU',
        'access_token': '1143362789160996864-agemUffwg40LkSrjTvT3p5JqyZK5Eq',
        'access_token_secret': 'PY4kmNFlgarC1pRFsjgRv4UPBcyShtmDAGDOaB2LWrxwA',
    }
    bom = botometer.Botometer(wait_on_ratelimit=True, rapidapi_key=rapidapi_key, **twitter_app_auth)
    #print(bom.check_account('Cardenas0Daniel'))
    users = []
    with open(os.path.join(carpeta, nodelist), 'rU', encoding="utf8") as File:
        reader = csv.DictReader(File)
        for i, line in enumerate(reader):
            user['Username'] = line['Username']
            user['Line'] = line
            try:
                result = bom.check_account(user['Username'])
                user['Botometer'] = result['scores']['universal']
            except:
                user['Botometer'] = 0
            print (str(i) + " " + user['Username'] + " " + str(user['Botometer']))

    with open('Botometer ' + nombre_salida + '.csv', 'wb', encoding="utf8") as File:
        writer = csv.writer(File)
        writer.writerow(
            ['Clusters (2)','Username','Number of Records'])
        for u in users:
            writer.writerow([u['Line'], u['Botometer']])


#Crea la matriz de probabilidades basándose en las menciones
# noinspection PyTypeChecker
def crear_matriz_conexiones(users, edges, nombre_proyecto, days_filter_lower=None, days_filter_upper=None):
    print("--------------------------------")
    print(nombre_proyecto)
    print(str(len(users)) + " users")
    print(str(len(edges)) + " edges")
    print(str(days_filter_lower) + " to " + str(days_filter_upper))
    matrix = numpy.zeros(shape=(len(users),len(users)))
    # i is source and j is target
    for e in edges:
        source = e[0] #El que menciona
        target = e[1] #A quien mencionaron
        matrix[source][target] = matrix[source][target] + 1

    print("-------------")
    filter_str = '' if days_filter_lower is None and days_filter_upper is None else 'filter('+str(days_filter_lower)+' '+str(days_filter_upper)+ ') '
    numpy.savetxt('Conexiones ' + filter_str + nombre_proyecto + '.csv', matrix, delimiter=';', encoding="utf8", fmt='%d')

#Crea la matriz de probabilidades basándose en las menciones
# noinspection PyTypeChecker
def crear_long_conexiones(users, edges, nombre_proyecto, days_filter_lower=None, days_filter_upper=None):
    print("-----------long---------------------")
    print(nombre_proyecto)
    print(str(len(users)) + " users")
    print(str(len(edges)) + " edges")
    print(str(days_filter_lower) + " to " + str(days_filter_upper))
    longs = [{} for x in range(len(users))]
    for e in edges:
        source = e[0]  # El que menciona
        target = e[1]  # A quien mencionaron
        cs = [0, 0, 0, 1] # el último es el número de tweets
        if e[11]:  # C1
            cs[0] = 1
        if e[12]:  # C2
            cs[1] = 1
        if e[13]:  # C3
            cs[2] = 1
        if target in longs[source].keys():
            list(map(add, longs[source][target], cs))
        else:
            longs[source].update({target:cs})

    print("-------------")
    filter_str = '' if days_filter_lower is None and days_filter_upper is None else 'filter('+str(days_filter_lower)+' '+str(days_filter_upper)+ ') '
    with open('Conex long ' + filter_str + nombre_proyecto + '.csv', 'w', encoding="utf8", newline='') as File:
        writer = csv.writer(File, delimiter=';')
        writer.writerow(['source', 'target', 'source_username', 'target_username', 'menciones_c1', 'menciones_c2', 'menciones_c3', 'num_tweets', 'grupo_source', 'grupo_target'])
        for source, d in enumerate(longs):
            gs = users[source]
            source_username = gs[1]
            grupo_source = 1
            if gs[3] <= gs[4] and gs[5] <= gs[4]:
                grupo_source = 2
            elif gs[3] <= gs[5]:
                grupo_source = 3

            if gs[3] == gs[4] == gs[5] == 0:
                grupo_source = 0

            for target in d.keys():
                gt = users[target]
                target_username = gt[1]
                grupo_target = 1
                if gt[3] <= gt[4] and gt[5] <= gt[4]:
                    grupo_target = 2
                elif gt[3] <= gt[5]:
                    grupo_target = 3

                if gt[3] == gt[4] == gt[5] == 0:
                   grupo_target = 0

                writer.writerow([source, target, source_username, target_username, d[target][0], d[target][1], d[target][2], d[target][3], grupo_source, grupo_target])


def to_sentiment(archivo_tweets, focus_words):
    max_int = sys.maxsize
    tweets_print = []
    tweets_words = []
    with open(archivo_tweets+".csv", 'rU', encoding="utf8") as File:
        tweets = csv.DictReader(File,delimiter=";")
        texts = []
        while True:
            # decrease the maxInt value by factor 10
            # as long as the OverflowError occurs.
            try:
                csv.field_size_limit(max_int)
                break
            except OverflowError:
                max_int = int(max_int / 10)
        for tweet in tweets:
            if bool([fw for fw in focus_words if(fw in tweet['text'])]) and tweet['text'] not in texts:
                tweet['text'] = re.sub(r'[\n+]', ' ', tweet['text'])
                tweet['eng'] = ""
                tweet['index'] = len(texts)
                tweet['polarity'] = None
                tweet['subjectivity'] = None
                #tweet['timeset_count'] = tweet['timeset'].count('T')
                #tweet['timeset'] = tweet['timeset'].split('Z')[0].replace("<[", "") + "Z"

                analysis = TextBlob(str(tweet['text']))
                print(tweet['text'])
                texts.append(tweet['text'])
                time.sleep(30)
                try:
                    eng = analysis.translate(to='en')
                    tweet['eng'] = eng.string
                    tweet['polarity'] = eng.sentiment.polarity
                    tweet['subjectivity'] = eng.sentiment.subjectivity
                    print(tweet['eng'] + " - " + str(tweet['polarity']))
                except Exception as e:
                    # Mostramos este mensaje en caso de que se presente algún problema
                    print("El elemento no está presente "+ str(e))
                tweets_print.append(tweet)
                text = re.sub(r'[@,.!?¿¡/#;:()\'’"”“👉]', '', tweet['text'])
                words = text.lower().replace('á','a').replace('é','e').replace('í','i').replace('ó','o').replace('ú','u').replace('s ',' ').split()
                words = list(dict.fromkeys(words))
                for word in words:
                    if len(word) > 4 and not 'http' in word:
                        temp = tweet.copy()
                        temp['palabra'] = word
                        tweets_words.append(temp)
    File.close()

    header = ['\ufeffid',"time","created_at","from_user_name","text","filter_level","possibly_sensitive","withheld_copyright",
                  "withheld_scope","truncated","retweet_count","favorite_count","lang","to_user_name","in_reply_to_status_id",
                  "quoted_status_id","source","location","lat","lng","from_user_id","from_user_realname","from_user_verified",
                  "from_user_description","from_user_url","from_user_profile_image_url","from_user_utcoffset","from_user_timezone",
                  "from_user_lang","from_user_tweetcount","from_user_followercount","from_user_friendcount","from_user_favourites_count",
                  "from_user_listed","from_user_withheld_scope","from_user_created_at",'eng', 'polarity', 'subjectivity', 'index']

    '''header = ['index', 'Id', 'text', 'timeset', 'twitter_type', 'lat', 'lng', 'place_country', 'place_type',
                  'place_fullname', 'place_name',
                  'created_at', 'lang', 'possibly_sensitive', 'quoted_status_permalink', 'description', 'email',
                  'profile_image',
                  'friends_count', 'followers_count', 'real_name', 'location', 'emoji_alias', 'emoji_html_decimal',
                  'emoji_utf8',
                  'cleaned_label', 'eng', 'polarity', 'subjectivity']'''

    with open('Sentimiento ' + archivo_tweets + '.csv', 'w', encoding="utf8", newline='') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerow(header)
        for tweet in tweets_print:
            t = []
            for h in header:
                t.append(str(tweet[h]).replace(';', '').replace('.', ','))
            writer.writerow(t)
        file.close()

    with open('Palabras ' + archivo_tweets + '.csv', 'w', encoding="utf8", newline='') as file:
        writer = csv.writer(file, delimiter=';')
        header.append('palabra')
        writer.writerow(header)
        for tweet in tweets_words:
            t = []
            for h in header:
                t.append(str(tweet[h]).replace(';', '').replace('.', ','))
            writer.writerow(t)
        file.close()


def get_usersa_posts_by_hashtag_ig(query):
    loader = Instaloader()
    NUM_POSTS = 10
    posts = loader.get_hashtag_posts(query)
    users = []
    count = 0
    for post in posts:
        print(post)
        profile = post.owner_profile
        caption = post.caption
        users.append([profile, caption])
        count += 1
        if count == NUM_POSTS:
            break
    return users


def get_hashtags_posts(query):
    posts = loader.get_hashtag_posts(query)
    prof_bio=[]
    count = 0
    for post in posts:
        #print(post)
        #caption=post.caption
        postprofile=post.profile
        bio=Profile.from_username(loader.context, postprofile)
        biopf=bio.biography
        prof_bio.append([postprofile,biopf])
        count += 1
        if count == NUM_POSTS:
                break
    return prof_bio



if __name__ == "__main__":
    main()