import pandas as pd
import requests
import datetime
import json
import io
import os
from urllib.parse import quote_plus
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, event
import pyodbc
import itertools
import re

print("xd")

#variables
today = datetime.date.today()
daysvar = -2
today = today - datetime.timedelta(days=daysvar)
Liga_Search = 'ESL Impact'

all_records = []

#note #1 remember how to substract days daysvar = daysvar - 1

url = "https://sports-api.cloudbet.com/pub/v2/odds/fixtures?"

querydict = {
    "sport":"counter-strike",
    "date":today,
}

headers = {
  'Accept': 'application/json',
  'X-API-Key': 'eyJhbGciOiJSUzI1NiIsImtpZCI6Img4LThRX1YwZnlUVHRPY2ZXUWFBNnV2bktjcnIyN1YzcURzQ2Z4bE44MGMiLCJ0eXAiOiJKV1QifQ.eyJhY2Nlc3NfdGllciI6ImFmZmlsaWF0ZSIsImV4cCI6MTk2NjI3NTQzOCwiaWF0IjoxNjUwOTE1NDM4LCJqdGkiOiIyNTc2OGMxMC0yODJkLTQ2ZGItYWMwYy04OThiZWE5YjEwZDgiLCJzdWIiOiIyNTgwNDQ1NC0wMjUxLTQ5MzMtODg3MC05MjdhODUwYTcxMTIiLCJ0ZW5hbnQiOiJjbG91ZGJldCIsInV1aWQiOiIyNTgwNDQ1NC0wMjUxLTQ5MzMtODg3MC05MjdhODUwYTcxMTIifQ.LNxQpK7l69v7-UbraNdweDOvwX7U7psC-jsMsUFbT83jQvv-Q0_iwJhiZmFxb3C8PWeiX_npOLzRqqYAOGU8-CHWbGjWNSn39IgIcp78TJba1ynbk1alwG7wcNYFAAyENbNaeNS_S7DMI8eKn9LL3mnPxBA6EaYfborKTQClLXswhrpgufp8W4vYs-YTTFFjGWX-UHzZm4Kf8rrnkotbDKvHtxzncMqO4yUEJtrBKz2LTINK5YyKc_WYt00aX1LiM311bRV8bWtjkmyR62wo6BNA5jHXOGWDcGOQkUVT78BDuACkG81R01YBaSB4XJXcA5xUTiFTg1T8pGLB9JtQAg'
}

while True :
    response = requests.get(url, headers=headers, params=querydict)
    response = response.content
    val1 = json.loads(response)
    if len(val1["competitions"]) == 0:
        print("Finished")
        break #break at empty results
    else:
        try:
            json_data = val1["competitions"]
            all_records = all_records + json_data
            daysvar += 1
            querydict["date"] = querydict['date'] - datetime.timedelta(days=daysvar)
            print("Downloading Data into All_records" + " " +  today) #keep looping for next page
        except TypeError:
            pass

df1 = pd.json_normalize(all_records, record_path=["events"])
df1 = df1.rename(columns={'cutoffTime' : 'time'})

df2 = pd.json_normalize(all_records).explode("events").reset_index(drop=True)
df2 = df2.drop("events", axis=1)
df2 = df2.rename(columns={'name' : 'league.name', 'key' : 'tournament.key', 'cutoffTime' : 'time'})
df3 = pd.concat([df1, df2], axis = 1, join='inner')

df3 = df3[df3['league.name'].str.contains(Liga_Search)==True]
df3["Source"] = "Pinnacle"
dfPinnacle = df3

#############################################################################################################betsapi

#Creating API call from betsAPI.com
url = "https://api.b365api.com/v3/events/upcoming?"

querydict = {
    "page":1,
    "token":"76516-9Mk5bHON84dqo3",
    "sport_id":"151",
}
payload = {}

all_records = []

response = requests.get(url, params=querydict)

#Create loop due to page limitations
while True :
    response = requests.get(url, params=querydict)
    response = response.content
    val1 = json.loads(response)
    if len(val1["results"]) == 0:
        print("Finished")
        querydict["page"] = 1
        break #break at empty results
    else:
        json_data = val1["results"]
        all_records = all_records + json_data
        querydict["page"] += 1
        print("Downloading Data into All_records" + " " +  str(querydict["page"])) #keep looping for next page


#Normalize the data because it's nested in Json format.
df = pd.json_normalize(all_records)
df['time'] = pd.to_datetime(df['time'],unit='s')
#Using filters to find the league I want notifcation = filter table.
df = df[df['league.name'].str.contains(Liga_Search)==True]
df["Source"] = "bet365"
dfBet365 = df

##########################################################################################################betfair


today = datetime.date.today()
print(today)
today = str(today)
r = requests.get('https://www.betfair.com/sport/e-sports/csgo-esl-impact-league/12468575')

find_teams = re.findall('class="team-name".*', r.text)

length_scraping = range(len(find_teams))
length_verify = len(find_teams)

if length_verify != 0:
    games = []

    for i in length_scraping:
        team_name = find_teams[i]
        team_name = team_name[25:-2]
        games.append(team_name)

    column_names = ["home.name", "away.name"]

    df_teams = pd.DataFrame(columns=column_names)

    for i in length_scraping[::2]:
        pd_series = pd.Series(games[i:i + 2], index=df_teams.columns)
        df_teams = df_teams.append(pd_series, ignore_index=True)

    length_scraping = range(len(df_teams))

    column_names_other = ["id", "time", "Source", "league.name"]

    df_rest = pd.DataFrame(columns=column_names_other)

    betfair_name = "betfair"

    id_list = []
    time_list = []
    source_list = []
    league_list = []
    for i in length_scraping:
        id = ["{i}{betfair_name}".format(i=i, betfair_name=betfair_name)]
        time = [today]
        source = ["betfair"]
        league_name = ["ESL Impact"]
        id_list.append(id)
        time_list.append(time)
        source_list.append(source)
        league_list.append(league_name)

    id_list = list(itertools.chain(*id_list))
    time_list = list(itertools.chain(*time_list))
    source_list = list(itertools.chain(*source_list))
    league_list = list(itertools.chain(*league_list))

    df_rest["id"] = id_list
    df_rest["time"] = time_list
    df_rest["Source"] = source_list
    df_rest["league.name"] = league_list

    frames = [df_rest, df_teams]

    df_all = pd.concat(frames, axis=1)
    dfBetfair = df_all[["id", "time", "home.name", "away.name", "league.name", "Source"]]
else:
    print("no data betfair")
########################################################################################################MergedDf
dfBet365 = dfBet365[["id","time","home.name","away.name","league.name","Source"]]
dfPinnacle = dfPinnacle[["id","time","home.name","away.name","league.name","Source"]]
dfBetfair = df_all[["id","time","home.name","away.name","league.name","Source"]]

MergedDf = pd.concat([dfBet365,dfPinnacle,dfBetfair])
MergedDf = pd.concat([dfBet365,dfPinnacle,dfBetfair]).reset_index(drop=True)

length_loop = len(MergedDf) #defining the length of dataframe to understand how many mails I have to send and how many matches there is.

####################################insert into SQL server to replicate second table.
# azure sql connect tion string
conn ='Driver={ODBC Driver 17 for SQL Server};Server=tcp:oddsalert.database.windows.net,1433;Database=oddsalert;Uid=admin01;Pwd=4w4e4r4tA!;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
quoted = quote_plus(conn)
engine=create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))

@event.listens_for(engine, 'before_cursor_execute')
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    print("FUNC call")
    if executemany:
        cursor.fast_executemany = True


#############################check if there is any new compared to the other dataframe
table_name = 'oddsalert_actual'
MergedDf.to_sql(table_name, engine, index=False, if_exists='replace', schema='dbo')

query = 'SELECT * FROM   oddsalert_actual as a WHERE  NOT EXISTS (SELECT * FROM  oddsalert_junk as b WHERE  a.id = b.id)'
dfsql = pd.read_sql(query, engine)

table_name = 'oddsalert_junk'
MergedDf.to_sql(table_name, engine, index=False, if_exists='replace', schema='dbo')


FinalDf = dfsql

length_loop = len(FinalDf)

#post call to slack
url = "https://slack.com/api/chat.postMessage" #slack link
channel = 'C03DRUPLRB2'

headers = {
  'Content-Type': 'application/json',
  'Authorization':'Bearer xoxb-3437572259414-3456892584289-gAPbruDdLbjmCj04Z9Kgo6Uz'
}


#looping the amounts of rows from filter and send each mail seperately
for x in range(length_loop):
    if length_loop != 0:
        row = FinalDf.iloc[x]
        league_name = row["league.name"]
        home_name = row["home.name"]
        away_name = row["away.name"]
        ID = row["id"]
        date = row["time"]
        Source = row["Source"]
        data =  {
            "channel": "{channel}".format(channel=channel),
            "text": "{ID} - {date} - New Bet : {league_name} - {home_name} VS {away_name} - {Source}".format(ID=ID, league_name = league_name, home_name = home_name, away_name = away_name, Source=Source, date=date)
        }
        response = requests.post(url, json=data, headers=headers)