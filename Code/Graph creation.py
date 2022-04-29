#!/usr/bin/env python
# -*- coding: utf-8 -*-
import csv
from datetime import datetime
from operator import add
from os.path import exists


def main():
    root_path = "Path to folder with data"
    project_data = get_elections_data('_Election data.csv')
    print(len(project_data.keys()), ' elections')
    for country_election in sorted(project_data.keys()):
        for i in range(-151, 0, 30):
            if not exists(root_path + "//Nodes and edges//Nodes " +
                          country_election + 'filter(' + str(i+1) + ' ' + str(i+30) + ') .csv'):
                print(country_election, i + 1, i + 30, 'missing')
                graph_creation(root_path + country_election + '.csv', project_data[country_election], country_election,
                               root_path + "//Nodes and edges//", days_filter_lower=i + 1, days_filter_upper=i + 30)
            else:
                print(country_election, i + 1, i + 30, 'already fulfilled')


# Gets the information needed to create the graph from the election metadata
def get_elections_data(elections_file):
    project_data = {}
    with open(elections_file, 'r', encoding="utf8") as File:
        reader = csv.DictReader(File)
        for election in reader:
            if election['has_issues'] == '':
                country_election = election['country'] + ' ' + str(election['year'])
                dmy = election['election_date'].split('/')
                project_data[country_election] = [datetime(day=int(dmy[0]), month=int(dmy[1]), year=int(dmy[2])),
                                                  election['@ winner'], election['@ second'], election['@ third']]
    return project_data


# Creates the files needed for the analysis
def graph_creation(tweets_file, project_data, country_election, results_path, days_filter_lower=None,
                   days_filter_upper=None):
    users = []  # user characteristics to export
    user_candidates = []  # has each candidate mentions and a fourth item for the total of tweets
    user_first = []  # first time the user tweets or receives a mention
    user_last = []  # last time the user tweets or receives a mention
    edges = []  # mention characteristics to export
    date_pv = project_data[0]  # date of election
    candidate_1 = str.lower(project_data[1])  # username of the winner candidate
    candidate_2 = str.lower(project_data[2])  # username of the runner-up candidate
    candidate_3 = str.lower(project_data[3])  # username of the third candidate

    with open(tweets_file, 'r', encoding="utf8") as File:
        reader = csv.DictReader(File)
        for tweet in reader:
            # Elimina tweets sin id de usuario
            if tweet['Permalink'] and tweet['Permalink'].count('/') > 0 and tweet['Mentions']:
                # Limpieza de tweets
                tweet['Text'] = str.lower(tweet['Text'].replace("http", " http")
                                          .replace("pic.twitter", " pic.twitter")
                                          .replace("&quot", " ").replace(";", " ")
                                          .replace("#", " #").replace("@", " @"))
                # Conseguir la date
                date = tweet['Date'].split(' ')
                ymd = date[0].split('/')
                hm = date[1].split(':')
                date_dt = datetime(day=int(ymd[2]), month=int(ymd[1]), year=int(ymd[0]), hour=int(hm[0]),
                                   minute=int(hm[1]))
                delta_pv = (date_dt - date_pv).days
                # Booleans to know which candidate(s) are mentioned on the tweet
                c1 = candidate_1 in tweet['Text'] and candidate_1 != ''
                c2 = candidate_2 in tweet['Text'] and candidate_2 != ''
                c3 = candidate_3 in tweet['Text'] and candidate_3 != ''
                # Mentions at least one fo the candidates and the tweet was done between 150 and 0 days before election
                if (c1 or c2 or c3) and delta_pv >= -150 and (days_filter_lower is None and days_filter_upper is None) \
                        or (days_filter_lower <= delta_pv <= days_filter_upper):
                    cs = [0, 0, 0, 1]
                    if c1:
                        cs[0] = 1
                    if c2:
                        cs[1] = 1
                    if c3:
                        cs[2] = 1

                    user = '@' + tweet['Permalink'].split('/')[1]
                    if user not in users and user != '@':
                        users.append(user)
                        user_candidates.append([0, 0, 0, 0])
                        user_first.append(delta_pv)
                        user_last.append(delta_pv)
                    # User filter
                    user_candidates[users.index(user)] = list(map(add, user_candidates[users.index(user)], cs))
                    # Date of first and last tweet
                    if user_first[users.index(user)] > delta_pv:
                        user_first[users.index(user)] = delta_pv
                    else:
                        user_last[users.index(user)] = delta_pv
                    # Mentioned users
                    mentions = tweet['Mentions'].split(' ')
                    for m in mentions:
                        if m != '':
                            m = '@' + m
                            if m not in users:
                                users.append(m)
                                user_candidates.append([0, 0, 0, 0])
                                user_first.append(delta_pv)
                                user_last.append(delta_pv)

                            if user_first[users.index(m)] > delta_pv:
                                user_first[users.index(m)] = delta_pv
                            else:
                                user_last[users.index(user)] = delta_pv

                            # Mentions filter
                            if m == '@' + tweet['Reply To User Name']:  # Determina si es una réplica al usuario
                                edges.append(
                                    [users.index(user), users.index(m), date_dt, tweet['Text'], tweet['Retweets'],
                                     tweet['Favorites'], 'True', delta_pv, c1, c2, c3])
                            else:
                                edges.append(
                                    [users.index(user), users.index(m), date_dt, tweet['Text'], tweet['Retweets'],
                                     tweet['Favorites'], 'False', delta_pv, c1, c2, c3])
                    print('users: ', len(users), ' edges: ', len(edges)) if len(users) % 10000 == 0 else None

    print("1-------------")

    filter_str = '' if days_filter_lower is None and days_filter_upper is None else 'filter(' + str(
        days_filter_lower) + ' ' + str(days_filter_upper) + ') '
    with open(results_path + 'Edges ' + country_election + filter_str + '.csv', 'w', encoding="utf8", newline='') \
            as File:
        writer = csv.writer(File, delimiter=';')
        writer.writerow(['source', 'target', 'date', 'text', 'retweets', 'favorites', 'is_reply',
                         'days_to_election', 'candidate_1', 'candidate_2', 'candidate_3'])
        for e in edges:
            print(e)
            writer.writerow(e)

    print("2-------------")

    with open(results_path + 'Nodes ' + country_election + filter_str + '.csv', 'w', encoding="utf8", newline='') \
            as File:
        writer = csv.writer(File, delimiter=';')
        writer.writerow(['id', 'username', 'first_tweet', 'last_tweet', 'mentions_c1', 'mentions_c2', 'mentions_c3',
                         'total_tweets'])
        users_print = []
        for i, screen_name in enumerate(users):
            if screen_name.startswith('@'):
                print(screen_name + " " + str(i))
                up = [i, screen_name, str(user_first[i]), str(user_last[i]),
                      user_candidates[i][0], user_candidates[i][1],
                      user_candidates[i][2], user_candidates[i][3]]
                writer.writerow(up)
                users_print.append(up)

    return {'users': users_print, 'edges': edges}


if __name__ == "__main__":
    main()
