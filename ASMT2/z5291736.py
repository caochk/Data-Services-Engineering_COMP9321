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
# name = api.model('name', {'name': fields.String})


# def handle_db():


# def create_db():
#     tv = requests.get('http://http://api.tvmaze.com/search/shows?q=TITLE')
#     data = tv.json()
#
#     con = sqlite3.connect('z5291736.db')
#     cur = con.cursor()
#     cur.execute('CREATE TABLE tvData (name text)')

def tvData_to_dataFrame(tvData, id):
    columns = ["id", "tvmaze_id", "name", "links_previous", "links_current", "links_next", "type", "language", "status", "runtime", "premiered",
               "officialSite", "weight", "genres", "schedule_time", "schedule_days", "rating", "network_id",
               "network_name", "network_country_name", "network_country_code", "network_country_timezone", "summary", "last_update"]
    df_tv = pd.DataFrame(columns=columns)
    tvData_row = []
    rowIndex = 0
    for tvData_element in tvData:
        tvData_row.append(id)
        tvData_row.append(tvData_element["show"]["id"])
        tvData_row.append(tvData_element["show"]["name"])

        tvData_ele_link = tvData_element["show"]["_links"]
        if 'previousepisode' in tvData_ele_link:
            # print("preEXIST!\n")
            tvData_row.append(tvData_element["show"]["_links"]["previousepisode"]["href"])
        else:
            # print("preNO!\n")
            tvData_row.append("None")
        tvData_row.append(tvData_element["show"]["_links"]["self"]["href"])
        if 'nextepisode' in tvData_ele_link:
            # print("nextEXIST!\n")
            tvData_row.append(tvData_element["show"]["_links"]["nextepisode"]["href"])
        else:
            # print("nextNO!\n")
            tvData_row.append("None")

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

        #因为network键下面可能就没东西了，所以得先判断
        tvData_ele_network = tvData_element["show"]["network"]
        if tvData_ele_network != None:
            tvData_row.append(tvData_element["show"]["network"]["id"])
            tvData_row.append(tvData_element["show"]["network"]["name"])
            tvData_row.append(tvData_element["show"]["network"]["country"]["name"])
            tvData_row.append(tvData_element["show"]["network"]["country"]["code"])
            tvData_row.append(tvData_element["show"]["network"]["country"]["timezone"])
        else:
            tvData_row.append("None")
            tvData_row.append("None")
            tvData_row.append("None")
            tvData_row.append("None")
            tvData_row.append("None")

        tvData_row.append(tvData_element["show"]["summary"])

        # last_update(creation time)
        time = datetime.datetime.now()
        date_str = time.strftime("%Y-%m-%d %H:%M:%S")
        tvData_row.append(date_str)

        # print("\ndf_tv:", df_tv)
        # print("\ntvROW:", tvData_row)

        df_tv.loc[rowIndex] = tvData_row
        tvData_row = []
        rowIndex += 1

    return df_tv





@api.route('/tv-shows/import')
class question1(Resource):
# question 1
    @api.response(404, 'Name of this TV show does not exist')
    @api.response(201, 'Created')
    # @api.response(200, 'OK')
    @api.param('name', 'Eg : good girl', methods=['POST'], type=str, required=True)
    def post(self):
        name = request.args.get('name') # the name of a TV show searched by users
        print(f"Name of TV show searched by users: {name}") # Used to display prompt information on the server
        tv = requests.get(f'http://api.tvmaze.com/search/shows?q={name}')
        tvData = tv.json()

        con = sqlite3.connect('z5291736.db')
        cur = con.cursor()
        cur.execute('CREATE TABLE IF NOT EXISTS tvTable (id integer, tvmaze_id integer, name text, links_previous text, links_current text, links_next text, '
                    'type text, language text, '
                    'status text, runtime text, premiered text, officialSite text, weight integer, genres text, schedule_time text, schedule_days text, '
                    'rating text, network_id integer, network_name text, network_country_name text, network_country_code text, network_country_timezone text, '
                    'summary text, last_update text)')
        # solve id issue [RIGHT!]
        cur.execute('select max(id) from tvTable')
        max_id = cur.fetchall() # fetchall返回的是一个二维列表
        if max_id[0][0] == None: #说明tvTable里面没有任何元组
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


@api.param('id', 'The identifier of every TV show in database')
@api.route('tv-shows/<id>')
class question2(Resource):
    # question2
    @api.response(400, 'Invalid name of TV show')
    @api.response(404, 'id not found')
    def get(self, id):
        # 以下代码是怕万一用户上来就直接执行get命令，那么此时未经过Q1的建表就根本没有表格供其查询，所以依然先建表（空不空无所谓）
        con = sqlite3.connect('z5291736.db')
        cur = con.cursor()
        cur.execute(
            'CREATE TABLE IF NOT EXISTS tvTable (id integer, tvmaze_id integer, name text, links_previous text, links_current text, links_next text, '
            'type text, language text, '
            'status text, runtime text, premiered text, officialSite text, weight integer, genres text, schedule_time text, schedule_days text, '
            'rating text, network_id integer, network_name text, network_country_name text, network_country_code text, network_country_timezone text, '
            'summary text, last_update text)')

        # 先判断用户输入的id本身是不是合理的：是否为整数、是否为正数
        if id < 1 or isinstance(id, int):
            return {'message': 'The id of this TV show is invalid.'}, 400

        # 再判断记录不存在的情况
        cur.execute(f"select id from tvTable where id = {id}")
        id_result = cur.fetchall()
        if id_result == []:
            return {'message': 'Cannot find a TV show whit this id.'}, 404
        else:
            cur.execute(f"select id, ")





if __name__ == '__main__':
    app.run(debug=True)