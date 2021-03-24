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

def tvData_to_dataFrame(tvData_element, id):
    columns = ["id", "tvmaze_id", "name", "links_previous", "links_current", "links_next", "type", "language", "status", "runtime", "premiered",
               "officialSite", "weight", "genres", "schedule_time", "schedule_days", "rating", "network_id",
               "network_name", "network_country_name", "network_country_code", "network_country_timezone", "summary", "last_update"]
    df_tv = pd.DataFrame(columns=columns)
    tvData_row = []
    # rowIndex = 0
    # for tvData_element in tvData:
    tvData_row.append(id)
    tvData_row.append(tvData_element["id"])
    tvData_row.append(tvData_element["name"])

    tvData_ele_link = tvData_element["_links"]
    if 'previousepisode' in tvData_ele_link:
        # print("preEXIST!\n")
        tvData_row.append(tvData_element["_links"]["previousepisode"]["href"])
    else:
        # print("preNO!\n")
        tvData_row.append("None")
    tvData_row.append(tvData_element["_links"]["self"]["href"])
    if 'nextepisode' in tvData_ele_link:
        # print("nextEXIST!\n")
        tvData_row.append(tvData_element["_links"]["nextepisode"]["href"])
    else:
        # print("nextNO!\n")
        tvData_row.append("None")

    tvData_row.append(tvData_element["type"])
    tvData_row.append(tvData_element["language"])
    tvData_row.append(tvData_element["status"])

    if tvData_element["runtime"] != None:
        tvData_row.append(tvData_element["runtime"])
    else:
        tvData_row.append("None")

    if tvData_element["premiered"] != None:
        tvData_row.append(tvData_element["premiered"])
    else:
        tvData_row.append("None")

    if tvData_element["officialSite"] != None:
        tvData_row.append(tvData_element["officialSite"])
    else:
        tvData_row.append("None")

    tvData_row.append(tvData_element["weight"])

    if tvData_element["genres"] == []:
        tvData_row.append("None")
    else:
        genresStr = ""
        count = 1
        for genresElement in tvData_element["genres"]:
            genresStr = genresStr + genresElement
            if count != len(tvData_element["genres"]):
                genresStr = genresStr + ','
                count += 1
        tvData_row.append(genresStr)

    if tvData_element["schedule"]["time"] == "":
        tvData_row.append("None")
    else:
        tvData_row.append(tvData_element["schedule"]["time"])

    if tvData_element["schedule"]["days"] == []:
        tvData_row.append("None")
    else:
        dayStr = ""
        count = 1
        for dayElement in tvData_element["schedule"]["days"]:
            dayStr = dayStr + dayElement
            if count != len(tvData_element["schedule"]["days"]):
                dayStr = dayStr + ','
                count += 1
        tvData_row.append(dayStr)

    if tvData_element["rating"]["average"] != None:
        tvData_row.append(tvData_element["rating"]["average"])
    else:
        tvData_row.append("None")

    #因为network键下面可能就没东西了，所以得先判断
    tvData_ele_network = tvData_element["network"]
    if tvData_ele_network != None:
        tvData_row.append(tvData_element["network"]["id"])
        tvData_row.append(tvData_element["network"]["name"])
        tvData_row.append(tvData_element["network"]["country"]["name"])
        tvData_row.append(tvData_element["network"]["country"]["code"])
        tvData_row.append(tvData_element["network"]["country"]["timezone"])
    else:
        tvData_row.append("None")
        tvData_row.append("None")
        tvData_row.append("None")
        tvData_row.append("None")
        tvData_row.append("None")

    tvData_row.append(tvData_element["summary"])

    # last_update(creation time)
    time = datetime.datetime.now()
    date_str = time.strftime("%Y-%m-%d %H:%M:%S")
    tvData_row.append(date_str)

    # print("\ndf_tv:", df_tv)
    # print("\ntvROW:", tvData_row)

    df_tv.loc[0] = tvData_row
    # tvData_row = []
    # rowIndex += 1

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
        tv = requests.get(f'http://api.tvmaze.com/singlesearch/shows?q={name}')
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
@api.route('/tv-shows/<int:id>')
class question2(Resource):
    # question2
    @api.response(400, 'Invalid id')
    @api.response(404, 'id not found')
    @api.response(200, 'Created')
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
        if id < 1 or isinstance(id, int) == False:
            # print("11111\n", type(id))
            return {'message': 'The id of this TV show is invalid.'}, 400

        # 再判断记录不存在的情况
        cur.execute(f"select id from tvTable where id = {id}")
        id_result = cur.fetchall()
        if id_result == []:
            return {'message': 'Cannot find a TV show whit this id.'}, 404
        else:
            cur.execute(f"select id, tvmaze_id, name, links_previous, links_current, links_next, type, language, status, runtime, premiered, "
                        f"officialSite, weight, genres, schedule_time, schedule_days, rating, network_id, "
                        f"network_name, network_country_name, network_country_code, network_country_timezone, summary, last_update from tvTable where id={id}")
            result = cur.fetchall()
            print("result:", result)
            id = result[0][0]
            tvmaze_id = result[0][1]
            name = result[0][2]
            links_previous = result[0][3]
            links_current = result[0][4]
            links_next = result[0][5]
            type = result[0][6]
            language = result[0][7]
            status = result[0][8]
            runtime = result[0][9]
            premiered = result[0][10]
            officialSite = result[0][11]
            weight = result[0][12]

            genres = result[0][13]
            genres_list = [] #genres = 'None'的情形
            if genres != "None" and "," not in genres: #genres = 'Anime'的情形
                genres_list.append(genres)
            if genres != "None" and "," in genres: #genres = 'Anime, Sci-fiction'的情形
                genres_list = genres.split(",")

            schedule_time = result[0][14]
            schedule_days = result[0][15]
            schedule_days_list = []  # schedule_days = 'None'的情形
            if schedule_days != "None" and "," not in schedule_days:  # schedule_days = 'Saturday'的情形
                schedule_days_list.append(schedule_days)
            if schedule_days != "None" and "," in schedule_days:  # schedule_days = 'Friday, Sunday'的情形
                schedule_days_list = schedule_days.split(",")

            rating = result[0][16]
            network_id = result[0][17]
            network_name = result[0][18]
            network_country_name = result[0][19]
            network_country_code = result[0][20]
            network_country_timezone = result[0][21]
            summary = result[0][22]
            last_update = result[0][23]

            return {
                "tvmaze_id": tvmaze_id, "id": id, "last-update": last_update, "name": name, "type": type, "language": language,
                "genres": genres_list, "status": status, "runtime": runtime, "premiered": premiered, "officialSite": officialSite,
                "schedule": {"time": schedule_time, "days": schedule_days_list}, "rating": {"average": rating}, "weight": weight,
                "network": {"id": network_id, "name": network_name, "country": {"name": network_country_name, "code": network_country_code,
                                                                               "timezone": network_country_timezone}},
                "summary": summary, "_links": {"self": {"href": links_current}, "previous": {"href": links_previous}, "next": {"href": links_next}}
            }, 200






if __name__ == '__main__':
    app.run(debug=True)