import json
import sqlite3
import requests
import re
import datetime
from flask import Flask, request
from flask_restx import Resource, Api, fields
import pandas as pd

app = Flask(__name__)
api = Api(app, title="Dataset for TV shows", description="This dataset allows\
                                                          clients to read and store some\
                                                         TV shows.")

# the model of name for the TV show
name = api.model('name', {'name': fields.String})


def handle_db():


def create_db():
    tv = requests.get('http://http://api.tvmaze.com/search/shows?q=TITLE')
    data = tv.json()

    con = sqlite3.connect('z5291736.db')
    cur = con.cursor()
    cur.execute('CREATE TABLE tvData (name text)')

def tvData_to_dataFrame(tvData, id):
    columns = ["id", "tvmaze_id", "name", "url", "type", "language", "status", "runtime", "premiered",
               "officialSite", "weight", "genres", "schedule_time", "schedule_days", "rating", "network_id",
               "network_name", "network_country_name", "network_country_code", "network_country_timezone", "summary", "last_update"]
    df_tv = pd.DataFrame(columns=columns)
    tvData_row = []
    i = 0
    for tvData_element in tvData:
        tvData_row.append(id)
        tvData_row.append(tvData_element["show"]["id"])
        tvData_row.append(tvData_element["show"]["name"])
        tvData_row.append(tvData_element["show"]["_links"])
        tvData_row.append(tvData_element["show"]["type"])
        tvData_row.append(tvData_element["show"]["language"])
        tvData_row.append(tvData_element["show"]["status"])
        tvData_row.append(tvData_element["show"]["runtime"])
        tvData_row.append(tvData_element["show"]["premiered"])
        tvData_row.append(tvData_element["show"]["officialSite"])
        tvData_row.append(tvData_element["show"]["weight"])

        genresStr = ""
        count = 1
        for genresElement in tvData_element["show"]["genres"]:
            genresStr = genresStr + genresElement
            if count != len(tvData_element["show"]["genres"]):
                genresStr = genresStr + ','
                count += 1
        tvData_row.append(genresStr)

        tvData_row.append(tvData_element["show"]["schedule"]["time"])

        dayStr = ""
        count = 1
        for dayElement in tvData_element["show"]["schedule"]["days"]:
            dayStr = dayStr + dayElement
            if count != len(tvData_element["show"]["schedule"]["days"]):
                dayStr = dayStr + ','
                count += 1
        tvData_row.append(dayStr)

        tvData_row.append(tvData_element["show"]["rating"]["average"])
        tvData_row.append(tvData_element["show"]["weight"])
        tvData_row.append(tvData_element["show"]["network"]["id"])
        tvData_row.append(tvData_element["show"]["network"]["name"])
        tvData_row.append(tvData_element["show"]["network"]["country"]["name"])
        tvData_row.append(tvData_element["show"]["network"]["country"]["code"])
        tvData_row.append(tvData_element["show"]["network"]["country"]["timezone"])
        tvData_row.append(tvData_element["show"]["summary"])

        # last_update(creation time)
        time = datetime.datetime.now()
        date_str = time.strftime("%Y-%m-%d %H:%M:%S")
        tvData_row.append(date_str)

        df_tv.loc[i] = tvData_row
        i += 1

    return df_tv





@api.route('/tv-shows/import')
class question1(Resource):
# question 1
    @api.response(404, 'Indicator id does not exist')
    @api.response(201, 'Create')
    @api.response(200, 'OK')
    def post(self):
        name = request.args.get('name') # the name of a TV show searched by users
        print(f"Name of TV show searched by users: {name}") # Used to display prompt information on the server
        tv = requests.get(f'http://http://api.tvmaze.com/search/shows?q={name}')
        tvData = tv.json()

        con = sqlite3.connect('z5291736.db')
        cur = con.cursor()
        cur.execute('CREATE TABLE IF NOT EXISTS tvTable (id integer, tvmaze_id integer, name text, url text, type text, language text, '
                    'status text, runtime text, premiered text, officialSite text, weight integer, genres text, schedule_time text, schedule_days text, '
                    'rating text, network_id integer, network_name text, network_country_name text, network_country_code text, network_country_timezone text'
                    'summary text, last_update text)')
        # solve id issue
        cur.execute('select max(id) from tvTable')
        max_id = cur.fetchall() # fetchall返回的是一个二维列表
        if len(max_id[0][0]) == 0: #说明tvTable里面没有任何元组
            id = 1
        else:
            id = int(max_id[0][0]) + 1 # 必须配合好上一个id

        # check if invalid name
        if tvData == []: # invalid
            return {'message': 'This TV show does not exist.'}, 404
        else: # valid
            df_tv = tvData_to_dataFrame(tvData, id)
            df_tv.to_sql('tvTable', con, if_exists='append', index=False)

            return {
                "id": df_tv.iloc[0, 0],
                "last-update": df_tv.iloc[0, -1],
                "tvmaze_id": df_tv.iloc[0, 1],
                "_links": {
                    "self": {
                        "herf": f"http://127.0.0.1:5000/tv-shows/{df_tv.iloc[0, 0]}"
                    }
                }
            }, 201









if __name__ == '__main__':
    app.run(debug=True)