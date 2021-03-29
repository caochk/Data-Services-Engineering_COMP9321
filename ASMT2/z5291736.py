import json
import sqlite3
import requests
import re
import datetime
from flask import Flask, request, send_file
from flask_restx import Resource, Api, fields
import pandas as pd
from math import ceil
import copy
from collections import Counter
import matplotlib.pyplot as plt

app = Flask(__name__)
api = Api(app, default="TV show", title="Dataset for TV shows", description="This dataset allows\
                                                          clients to read and store some\
                                                         TV shows.")


# the model of the TV show【方式2】【失败】【用于Q4】

# schedule = {}
# schedule['time'] = fields.String(attribute='time')
# schedule['days'] = fields.List(fields.String, attribute='days')
# schedule_payload = api.model('schedule', schedule)
#
# rating = {}
# rating['average'] = fields.String(attribute='average')
# rating_payload = api.model('rating', rating)
#
# country = {}
# country['name'] = fields.String(attribute='name')
# country['code'] = fields.String(attribute='code')
# country['timezone'] = fields.String(attribute='timezone')
# country_payload = api.model('country', country)
# network_payload = api.model('network', {
#     'id': fields.Integer,
#     'name': fields.String,
#     'country': fields.Nested(country_payload)
# })
#
# self = {}
# self['href'] = fields.String(attribute='href')
# self_payload = api.model('self', self)
# previous = {}
# previous['href'] = fields.String(attribute='href')
# previous_payload = api.model('previous', previous)
# next = {}
# next['href'] = fields.String(attribute='href')
# next_payload = api.model('next', next)
# links_payload = api.model('links', {
#     'self': fields.Nested(self),
#     'previous': fields.Nested(previous),
#     'next': fields.Nested(next)
# })
#
# tv_payload = api.model('tv', {
#     'tvmaze_id': fields.Integer,
#     'id': fields.Integer,
#     'last-update': fields.String,
#     'name': fields.String,
#     'type': fields.String,
#     'language': fields.String,
#     'genres': fields.List(fields.String),
#     'status': fields.String,
#     'runtime': fields.String,
#     'premiered': fields.String,
#     'officialSite': fields.String,
#     'schedule': fields.Nested(schedule_payload),
#     'rating': fields.Nested(rating_payload),
#     'weight': fields.Integer,
#     'network': fields.Nested(network_payload),
#     'summary': fields.String,
#     '_links': fields.Nested(links_payload)
# })

# the model of the TV show【方式1】【model中若没有字典套字典可以通过】【用于Q4】

tv_model = api.model('tv model', {
    'tvmaze_id': fields.Integer,
    'id': fields.Integer,
    'last-update': fields.String,
    'name': fields.String,
    'type': fields.String,
    'language': fields.String,
    'genres': fields.List(fields.String),
    'status': fields.String,
    'runtime': fields.String,
    'premiered': fields.String,
    'officialSite': fields.String,
    # 'schedule': {
    #     'time': fields.String,
    #     'days': fields.List(fields.String)
    # },
    # 'rating': {
    #     'average': fields.String,
    # },
    'weight': fields.Integer,
    # 'network': {
    #     'id': fields.Integer,
    #     'name': fields.String,
    #     'country': {
    #         'name': fields.String,
    #         'code': fields.String,
    #         'timezone': fields.String,
    #     },
    # },
    'summary': fields.String,
    # '_links': {
    #     'self': {
    #         'href': fields.String,
    #     },
    #     'previous': {
    #         'href': fields.String,
    #     },
    #     'next': {
    #         'href': fields.String,
    #     },
    # }
})


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
    date = datetime.datetime.now()
    date_str = date.strftime("%Y-%m-%d %H:%M:%S")
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
        cur.execute('SELECT max(id) FROM tvTable')
        max_id = cur.fetchall() # fetchall返回的是一个二维列表
        if max_id[0][0] == None: #说明tvTable里面没有任何元组
            id = 1
        else:
            id = int(max_id[0][0]) + 1 # 必须配合好上一个id

        # print("tvData:", tvData)
        # check if invalid name
        if tvData == None: # invalid
            # print("\nENTER\n")
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
class question2_3_4(Resource):
    # question2
    @api.response(400, 'Invalid id')
    @api.response(404, 'id not found')
    @api.response(200, 'Retrieved')
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
        cur.execute(f"SELECT id FROM tvTable WHERE id = {id}")
        id_result = cur.fetchall()
        if id_result == []:
            return {'message': 'Cannot find a TV show whit this id.'}, 404
        else:
            cur.execute(f"SELECT id, tvmaze_id, name, links_previous, links_current, links_next, type, language, status, runtime, premiered, "
                        f"officialSite, weight, genres, schedule_time, schedule_days, rating, network_id, "
                        f"network_name, network_country_name, network_country_code, network_country_timezone, summary, last_update FROM tvTable WHERE id={id}")
            result = cur.fetchall()
            # print("result:", result)
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

    # question3
    @api.response(400, 'Invalid id')
    @api.response(404, 'id not found')
    @api.response(200, 'Deleted')
    def delete(self, id):
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
        cur.execute(f"SELECT id FROM tvTable WHERE id = {id}")
        id_result = cur.fetchall()
        if id_result == []:
            return {'message': 'Cannot find a TV show with this id.'}, 404
        else:
            cur.execute(f'DELETE FROM tvTable WHERE id = {id}')
            con.commit()
            # print("111:", cur.fetchall()) #如果返回的为[]说明被删了

            return {
                "message": f"The tv show with id {id} was removed from the database!",
                "id": id
            }, 200

    # question4
    @api.response(400, 'Invalid id')
    @api.response(404, 'id not found')
    @api.response(200, 'Updated')
    @api.expect(tv_model)
    # @api.marshal_with(tv_payload)
    def patch(self, id):
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

        tv = request.json #取得payload并转换为json
        # print(type(tv)) #已确认为字典类型<class 'dict'>
        # print("\ntv.keys:", tv.keys()) #确实会打印出来那些我在sagger中写了的想要修改值的键
        if 'last-update' in tv:
            tv['last_update'] = tv.pop('last-update')  # 键名换成和数据库中一致的

        # print("\ntv.keys1:", tv.keys())



        # 再判断记录不存在的情况
        cur.execute(f"SELECT id FROM tvTable WHERE id = {id}")
        id_result = cur.fetchall()
        if id_result == []:
            return {'message': 'Cannot find a TV show whit this id.'}, 404
        else:
            # update the value
            for key in tv:
                # unexpected key
                # if key not in tv_model.keys() or key not in tv_model['schedule'].keys() or key not in tv_model['network'].keys()\
                #         or key not in tv_model['network']['country'].keys() or key not in tv_model['_links']:
                #     return {"message": "Property {} is invalid".format(key)}, 400

                cur.execute(f'UPDATE tvTable set {key} = {tv[key]} WHERE id = {id}')
                con.commit()


@api.response(400, 'Invalid input')
@api.response(404, 'No TV show')
@api.response(200, 'OK')
@api.param('order_by', 'a comma separated string value to sort the list')
@api.param('page', 'used for pagination')
@api.param('page_size', 'used for pagination')
@api.param('filter', 'used to show what attribute should be shown')
@api.route('/tv-shows')
class question5(Resource):
    def get(self):
        # 获取用户输入的参数，包括order_by,page, page, page_size, filter
        order_by = request.args.get('order_by')
        page = request.args.get('page')
        page_size = request.args.get('page_size')
        filter = request.args.get('filter')

        # 以下代码是怕万一用户上来就直接执行get命令，那么此时未经过Q1的建表就根本没有表格供其查询，所以依然先建表（空不空无所谓）
        con = sqlite3.connect('z5291736.db')
        cur = con.cursor()
        cur.execute(
            'CREATE TABLE IF NOT EXISTS tvTable (id integer, tvmaze_id integer, name text, links_previous text, links_current text, links_next text, '
            'type text, language text, '
            'status text, runtime text, premiered text, officialSite text, weight integer, genres text, schedule_time text, schedule_days text, '
            'rating text, network_id integer, network_name text, network_country_name text, network_country_code text, network_country_timezone text, '
            'summary text, last_update text)')

        # 用户未输入某参数时，则采用默认值
        if order_by == None:
            order_by = '+id'
        if page == None:
            page = '1'
        if page_size == None:
            page_size = '100'
        if filter == None:
            filter = 'id,name'

        # 对order_by进行处理，因为是由逗号分隔的字符串且包含了+、-号；filter也一样列表化
        # 先去除order_by和filter字符串内的空格，方便后续使用split
        order_by_no_space = ''
        for i in order_by:
            if i != ' ':
                order_by_no_space += i
            else:
                continue

        filter_no_space = ''
        for i in filter:
            if i != ' ':
                filter_no_space += i
            else:
                continue

        # 若由逗号隔开多个值则用split，不然直接放入列表
        order_by_list = order_by_no_space.split(',')
        filter_list = filter_no_space.split(',')

        # 检查所有输入的参数是否valid
        # 先检查page,page_size参数是否只由数字构成
        if page.isdigit() == False or page_size.isdigit() == False:
            return {'message': 'The page and page_size parameters can only be numeric.'}, 400 # 【通过】
        # 再检查order_by和filter参数是否包含了允许字符之外的字符
        standard_order_dy = ['+id', '-id', '+name', '-name', '+runtime', '-runtime', '+premiered', '-premiered',
                             '+rating-average', '-rating-average']
        standard_filter = ['tvmaze_id', 'id', 'last-update', 'name', 'type', 'language',
                           'genres', 'status', 'runtime', 'premiered', 'officialSite', 'schedule', 'rating',
                           'weight', 'network', 'summary']
        for i in order_by_list:
            if i not in standard_order_dy:
                return {'message': 'The order_by parameter is invalid.'}, 400 # 【通过】

        for i in filter_list:
            if i not in standard_filter:
                return {'message': 'The filter parameter is invalid.'}, 400 # 【通过】

        # 开始从sqlite中进行查询
        # 构建查询语句前的处理工作
        # filter_list的前序处理：扩列
        filter_query_string = ''
        count = 1
        for i in filter_list:
            if i == 'last-update':
                filter_query_string += 'last_update'
            elif i == 'schedule':
                filter_query_string += 'schedule_time,'
                filter_query_string += 'schedule_days'
            elif i == 'network':
                filter_query_string += 'network_id,'
                filter_query_string += 'network_name,'
                filter_query_string += 'network_country_name,'
                filter_query_string += 'network_country_code,'
                filter_query_string += 'network_country_timezone'
            else:
                filter_query_string += i

            if count < len(filter_list):
                filter_query_string += ','
            count += 1
        filter_query_list = filter_query_string.split(',') # 在后面return时要用，为了和fetchall返回的列表的成员个数对上
        # print("filter list:", filter_list)
        # print("filter query list", filter_query_list)

        # order_by_list的前序处理
        order_by_query_string = ''
        count = 1
        for i in order_by_list:
            order_by_query_string += i[1:]
            order_by_query_string += ' '
            if i[0] == '+':
                order_by_query_string += 'ASC'
            else:
                order_by_query_string += 'DESC'

            if count < len(order_by_list):
                order_by_query_string += ', '
            count += 1

        #测试
        # print("filter query:", filter_query_string)
        # print("order by query:", order_by_query_string)

        # 开始正式查询
        cur.execute(f"SELECT {filter_query_string} FROM tvTable ORDER BY {order_by_query_string}")
        result = cur.fetchall()
        # print("result:", result)
        # 没有任何电视剧记录的情形
        if result == []:
            return {'message': 'There are no TV shows in the table at present.'}, 404 #【通过】
        else:
            total_item_num = len(result)
            current_page_num = 0 # 用于在第二层循环中统计程序经过几个page了
            required_page = ceil(total_item_num/int(page_size))

            tv_shows_dict = dict()
            tv_shows_list = []
            final_dict = dict()
            final_list = []
            # genres_list = []
            schedule_dict = dict()
            rating_dict = dict()
            network_dict = dict()
            network_country_dict = dict()

            href_dict = dict()
            links_dict = dict()
            for i in range(int(page)):
                if i < required_page:
                    if total_item_num <= int(page_size):
                        real_page_size = total_item_num
                    else:
                        if total_item_num - current_page_num * int(page_size) >= int(page_size):
                            real_page_size = int(page_size)
                        else:
                            real_page_size = total_item_num - current_page_num * int(page_size)
                    # print("real page size:", real_page_size)
                    for j in range(real_page_size):
                        for attribute, ele in zip(filter_query_list, result[j + i * int(page_size)]):
                            # print("attribute:", attribute)
                            # print("ele:", ele)
                            if attribute == 'genres':
                                # print("1111111111111111111")
                                # genres_list.append()
                                # print("genres_list:", ele.split(','))
                                tv_shows_dict[attribute] = ele.split(',')
                            elif attribute == 'schedule_time':
                                schedule_dict["time"] = ele
                                # print("schedule dict1:", schedule_dict)
                            elif attribute == 'schedule_days':
                                schedule_dict["days"] = ele.split(',')
                                # print("schedule dict2:", schedule_dict)
                                tv_shows_dict["schedule"] = schedule_dict
                            elif attribute == 'rating':
                                rating_dict["average"] = ele
                                tv_shows_dict[attribute] = rating_dict
                            elif attribute == 'network_id':
                                network_dict["id"] = ele
                            elif attribute == 'network_name':
                                network_dict["name"] = ele
                            elif attribute == 'network_country_name':
                                network_country_dict["name"] = ele
                            elif attribute == 'network_country_code':
                                network_country_dict["code"] = ele
                            elif attribute == 'network_country_timezone':
                                network_country_dict["timezone"] = ele
                                network_dict["country"] = network_country_dict
                                tv_shows_dict["network"] = network_dict
                            else:
                                tv_shows_dict[attribute] = ele


                            # print("tv shows dict before:", tv_shows_dict)
                        tv_shows_dict_tmp = copy.deepcopy(tv_shows_dict)
                        tv_shows_list.append(tv_shows_dict_tmp)
                        # print("j:", j)
                        # print("tv_shows_dict:", tv_shows_dict)
                        # print("tv_shows_list:", tv_shows_list)
                    final_dict["page"] = i + 1
                    final_dict["page-size"] = page_size
                    final_dict["tv-shows"] = tv_shows_list
                    if i == 0:
                        href_dict["href"] = f"http://127.0.0.1:5000/tv-shows?page={i+1},page_size={page_size}"
                        href_dict_tmp = copy.deepcopy(href_dict)
                        links_dict["self"] = href_dict_tmp
                        # print(links_dict)
                        href_dict["href"] = f"http://127.0.0.1:5000/tv-shows?page={i+1+1},page_size={page_size}"
                        href_dict_tmp = copy.deepcopy(href_dict)
                        links_dict["next"] = href_dict_tmp
                        # print(links_dict)
                        final_dict["_links"] = links_dict
                    elif i > 0 and i < required_page - 1:
                        href_dict["href"] = f"http://127.0.0.1:5000/tv-shows?page={i+1},page_size={page_size}"
                        href_dict_tmp = copy.deepcopy(href_dict)
                        links_dict["self"] = href_dict_tmp
                        href_dict["href"] = f"http://127.0.0.1:5000/tv-shows?page={i},page_size={page_size}"
                        href_dict_tmp = copy.deepcopy(href_dict)
                        links_dict["previous"] = href_dict_tmp
                        href_dict["href"] = f"http://127.0.0.1:5000/tv-shows?page={i+1+1},page_size={page_size}"
                        href_dict_tmp = copy.deepcopy(href_dict)
                        links_dict["next"] = href_dict_tmp
                        final_dict["_links"] = links_dict
                    else:
                        href_dict["href"] = f"http://127.0.0.1:5000/tv-shows?page={i+1},page_size={page_size}"
                        href_dict_tmp = copy.deepcopy(href_dict)
                        links_dict["self"] = href_dict_tmp
                        href_dict["href"] = f"http://127.0.0.1:5000/tv-shows?page={i},page_size={page_size}"
                        href_dict_tmp = copy.deepcopy(href_dict)
                        links_dict["previous"] = href_dict_tmp
                        final_dict["_links"] = links_dict
                    # print("final dict:", final_dict)
                    final_dict_tmp = copy.deepcopy(final_dict)
                    final_list.append(final_dict_tmp)
                    # print("final list:", final_list)
                    tv_shows_list = []
                    links_dict = dict()

                    current_page_num += 1 # final_list里加了一个字典就表明一个page做完了
                else:
                    if "******Page parameter sets too large, the above page has shown all the available TV shows.******" not in final_list:
                        final_list.append("******Page parameter sets too large, the above page has shown all the available TV shows.******")
            return final_list, 200


@api.response(404, 'No TV show')
@api.response(200, 'OK')
@api.response(400, 'Invalid input')
@api.param('format', 'Eg: json or image')
@api.param('by', 'Eg: language, status, type or genres')
@api.route('/tv-shows/statistics')
class question6(Resource):
    def get(self):
        format = request.args.get('format')
        by = request.args.get('by')

        # 以下代码是怕万一用户上来就直接执行get命令，那么此时未经过Q1的建表就根本没有表格供其查询，所以依然先建表（空不空无所谓）
        con = sqlite3.connect('z5291736.db')
        cur = con.cursor()
        cur.execute(
            'CREATE TABLE IF NOT EXISTS tvTable (id integer, tvmaze_id integer, name text, links_previous text, links_current text, links_next text, '
            'type text, language text, '
            'status text, runtime text, premiered text, officialSite text, weight integer, genres text, schedule_time text, schedule_days text, '
            'rating text, network_id integer, network_name text, network_country_name text, network_country_code text, network_country_timezone text, '
            'summary text, last_update text)')

        # 检查所有输入的参数是否valid
        standard_format = ['json', 'image']
        standard_by = ['language', 'genres', 'status', 'type']
        if format not in standard_format:
            return {'message': 'The format parameter is invalid.'}, 400  #

        if by not in standard_by:
            return {'message': 'The by parameter is invalid.'}, 400  #

        # 开始查询数据库
        # 先获取total num of TV shows，若没有东西返回说明表为空
        cur.execute(f"SELECT id FROM tvTable")
        id_result = cur.fetchall()
        if id_result == []:
            return {'message': 'There are no TV shows in the table at present.'}, 404 #
        else:
            total_num = len(id_result) # total_num = total number of TV shows in database

        # 再获取total num of TV shows updated in 24 hours
        # 先获取24小时前的时间
        last_date = datetime.datetime.today() - datetime.timedelta(days=1)
        last_date_str = last_date.strftime('%Y-%m-%d %H:%M:%S')
        # print(last_date_str)

        cur.execute("SELECT id FROM tvTable WHERE last_update>=?", (last_date_str,))
        id_result = cur.fetchall()
        if id_result == []:
            return {'message': 'No TV shows have been updated in the past 24 hours.'}, 404 #
        else:
            total_num_in_24 = len(id_result)

        # 获取各类by参数
        # print(by)
        if by == 'language' or by == 'status' or by == 'type':
            cur.execute(f"SELECT {by} FROM tvTable")
            result = cur.fetchall()
            result_list = []
            for i in result:
                result_list.append(i[0])
            # print(result_list)
            count = dict(Counter(result_list))
            # print(count)
        else:
            cur.execute(f"SELECT {by} FROM tvTable")
            result = cur.fetchall()
            result_list = []
            for i in result:
                result_list.append(i[0])
            result_list_without_none = []
            for i in result_list:
                if i != 'None':
                    result_list_without_none.append(i)
            count_tmp = dict(Counter(result_list_without_none))
            total_genres = len(result_list)
            count = dict()
            for i in count_tmp:
                count[i] = count_tmp[i]/total_genres

        # json还是画图
        if format == 'json':
            return {
                "total": total_num,
                "total-updated": total_num_in_24,
                "values": count
            }, 200
        else:
            # 画第一个子图
            total_num_list_value = []
            total_num_list_key = []
            total_num_list_key.append("Total Num of TV shows")
            total_num_list_value.append(total_num)

            total_num_list_key.append("Total Num of TV shows (last 24 hours)")
            total_num_list_value.append(total_num_in_24)

            plt.figure(figsize=(8, 8), dpi=100)

            plt.subplot(2, 1, 1)
            plt.bar(total_num_list_key, total_num_list_value, align='center', width=0.1, alpha=0.8, color=('r', 'b'))
            # plt.legend()
            plt.ylabel("amount")
            # plt.xlabel("种类")
            # plt.title("条形图")

            # 画第二个子图（饼图）
            plt.subplot(2, 1, 2)
            label = list(count.keys())
            data = list(count.values())
            plt.pie(data, labels=label, radius=0.5, autopct='%3.2f%%')
            plt.axis('equal')  # 正圆
            plt.title(f"percentage of TV shows per {by}", loc='left', fontsize=16, fontweight='semibold')
            plt.legend(loc='upper right', bbox_to_anchor=(1.1, 1.05), fontsize=11, borderaxespad=0.3)

            plt.savefig("question6.png")
            # plt.show()
            return send_file("question6.png")








if __name__ == '__main__':
    app.run(debug=True)