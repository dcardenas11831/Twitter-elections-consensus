#!/usr/bin/env python
# -*- coding: utf-8 -*-
from TwitterAPI import TwitterAPI, TwitterOAuth, TwitterRequestError, TwitterConnectionError, TwitterPager
import csv
import json
import re
import time

def main():
    project = 'Colombia 2014'
    query = '(JuanManSantos OR OIZuluaga OR mluciaramirez)'
    query_time = ['2013-12-26T00:00:00.01Z', '2014-05-25T23:59:00.01Z']
    #twitter_app_auth = {
    #    'consumer_key': 'kjeenOn65kCsRrI3xEOB5VcAo',
    #    'consumer_secret': 'UWcNVR8T0JRaHOoHehiHgjb6YdViGLWagr1p603CiLgOSfrz1E',
    #    'access_token': '7080872-KjIidqCElHYNZVvxkxTEaWGhdaWJYfZB0k91k7ylr3',
    #    'access_token_secret': 'aPZeUiwSai42spTj11BfOI68J82C7PNWLPrliDb72b8Eh',
    #}
    twitter_app_auth = {
        'consumer_key': 'VnLAZNEKzPoTnQQXqyQwWT352',
        'consumer_secret': '1ZVKI2rWJwAMqqB9YUT1wFNa4KwQq3rtnTIgL7oAJwbWCVPs1l',
        'access_token': '1143362789160996864-q5ScDIU2jPD05P1WQBQlyhWupH8oGg',
        'access_token_secret': 'W6SRHXbN430oUcUZhErvRsKVbDi3MR7VzVJbNvjXvIduS',
    }

    users = {}
    tweets = []
    api = TwitterAPI(twitter_app_auth['consumer_key'], twitter_app_auth['consumer_secret'], auth_type='oAuth2', api_version='2')
    next_token = ''
    go = True
    while go:
        try:
            if next_token == '':
                r = api.request('tweets/search/all', {
                    'query': query,
                    'max_results': 500,
                    'start_time': query_time[0],
                    'end_time': query_time[1],
                    'tweet.fields': 'created_at,public_metrics,text,author_id,entities',
                    'user.fields': 'id,location,name,public_metrics,description',
                    'expansions': 'author_id,in_reply_to_user_id'})
            else:
                r = api.request('tweets/search/all', {
                    'query': query,
                    'max_results': 500,
                    'start_time': query_time[0],
                    'end_time': query_time[1],
                    'tweet.fields': 'created_at,public_metrics,text,author_id,entities',
                    'user.fields': 'id,location,name,public_metrics,description',
                    'expansions': 'author_id,in_reply_to_user_id',
                    'next_token': next_token})

            #print(r.json()['data'][0].keys())
            #print(r.json()['data'])

            if 'status' in r.json():
                print(r.json)
                print("Sleep error")
                time.sleep(900)
            else:
                meta = r.json()['meta']
                if 'next_token' not in meta:
                    go = False
                    break
                next_token = meta['next_token']
                includes = r.json()['includes']

                for inc in includes['users']:
                    author_id = inc['id']
                    if 'location' in inc:
                        users[author_id] = [inc['username'],inc['location'].replace("\n",'').replace(",",''),inc['description'].replace("\n",'').replace(",",'').replace('\x00',''),inc['public_metrics']['followers_count'],inc['public_metrics']['following_count'],inc['public_metrics']['tweet_count']]
                    else:
                        users[author_id] = [inc['username'],None,inc['description'].replace("\n",'').replace(",",'').replace('\x00',''),inc['public_metrics']['followers_count'],inc['public_metrics']['following_count'],inc['public_metrics']['tweet_count']]

                for item in r.json()['data']:
                    u = users[item['author_id']]
                    mentions = ''
                    if 'entities' in item:
                        entities = item['entities']
                        #print(entities)
                        if 'mentions' in entities:
                            for en in entities['mentions']:
                                #print(en)
                                mentions = mentions + ' ' + en['username']
                    tweet = {
                        'ID': item['id'],
                        'Permalink': '/'+u[0]+'/status/'+item['id'],
                        'Author ID': item['author_id'],
                        'Author Name': u[0],
                        'Author Location': u[1],
                        'Author Description': str(u[2]),
                        'Author Followers': u[3],
                        'Author Following': u[4],
                        'Author Tweets': u[5],
                        'Date': re.sub('.[0-9]*Z', '', item['created_at'].replace("T",' ').replace("-","/")),
                        'Text': item['text'].replace("\n",'').replace(",",''),
                        'Replies': item['public_metrics']['reply_count'],
                        'Retweets': item['public_metrics']['retweet_count'],
                        'Favorites': item['public_metrics']['like_count'],
                        'is Retweet?': item['text'].startswith('RT'),
                        'Reply To User Name': users[item['in_reply_to_user_id']][0] if 'in_reply_to_user_id' in item and item['in_reply_to_user_id'] in users else None,
                        'Mentions': mentions
                    }
                    tweets.append(tweet)

            print('\nQUOTA')
            print(r.get_quota())
            print('tweets: ' + str(len(tweets)) + ' users: ' + str(len(users)))

            with open(project+'.csv', 'w', encoding="utf8", newline='')  as output_file:
                dict_writer = csv.DictWriter(output_file, tweets[0].keys())
                dict_writer.writeheader()
                dict_writer.writerows(tweets)

            if r.get_quota()['remaining'] is None or r.get_quota()['remaining'] <= 1:
                print("sleep")
                time.sleep(900)


        except TwitterRequestError as e:
            print(e.status_code)
            for msg in iter(e):
                print(msg)

        except TwitterConnectionError as e:
            print(e)







if __name__ == "__main__":
    main()