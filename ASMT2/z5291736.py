import sqlite3
import requests
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


# the model of the TV show

schedule_payload = api.model('schedule', {
    'time': fields.String,
    'days': fields.List(fields.String)
})

rating_payload = api.model('rating', {
    'average': fields.String
})

country_payload = api.model('country', {
    'name': fields.String,
    'code': fields.String,
    'timezone': fields.String
})
network_payload = api.model('network', {
    'id': fields.Integer,
    'name': fields.String,
    'country': fields.Nested(country_payload)
})

tv_payload = api.model('tv', {
    'name': fields.String,
    'type': fields.String,
    'language': fields.String,
    'genres': fields.List(fields.String),
    'status': fields.String,
    'runtime': fields.String,
    'premiered': fields.String,
    'officialSite': fields.String,
    'schedule': fields.Nested(schedule_payload),
    'rating': fields.Nested(rating_payload),
    'weight': fields.Integer,
    'network': fields.Nested(network_payload),
    'summary': fields.String,
})



def tvData_to_dataFrame(tvData_element, id):
    columns = ["id", "tvmaze_id", "name", "links_previous", "links_current", "links_next", "type", "language", "status", "runtime", "premiered",
               "officialSite", "weight", "genres", "schedule_time", "schedule_days", "rating", "network_id",
               "network_name", "network_country_name", "network_country_code", "network_country_timezone", "summary", "last_update"]
    df_tv = pd.DataFrame(columns=columns)
    tvData_row = []
    tvData_row.append(id)
    tvData_row.append(tvData_element["id"])
    tvData_row.append(tvData_element["name"])

    tvData_ele_link = tvData_element["_links"]
    if 'previousepisode' in tvData_ele_link:
        tvData_row.append(tvData_element["_links"]["previousepisode"]["href"])
    else:
        tvData_row.append("None")
    tvData_row.append(tvData_element["_links"]["self"]["href"])
    if 'nextepisode' in tvData_ele_link:
        tvData_row.append(tvData_element["_links"]["nextepisode"]["href"])
    else:
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

    date = datetime.datetime.now()
    date_str = date.strftime("%Y-%m-%d %H:%M:%S")
    tvData_row.append(date_str)

    df_tv.loc[0] = tvData_row
    return df_tv


@api.route('/tv-shows/import')
class question1(Resource):
    # question 1
    @api.response(404, 'Name of this TV show does not exist')
    @api.response(201, 'Created')
    @api.response(200, 'Already exist')
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
        # solve id issue
        cur.execute('SELECT max(id) FROM tvTable')
        max_id = cur.fetchall()
        if max_id[0][0] == None: # It means that there are no tuples in tvtable
            id = 1
        else:
            id = int(max_id[0][0]) + 1

        # check if invalid name
        if tvData == None: # invalid
            # print("\nENTER\n")
            return {'message': 'This TV show does not exist.'}, 404
        else: # valid
            cur.execute("SELECT id FROM tvTable WHERE tvmaze_id=?", (tvData['id'],))
            existed_id = cur.fetchall()
            if existed_id != []:
                return {'message': 'This TV show already exists.'}, 200
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
        # If the user executes the get command directly, there will be no table to query without creating a table
        con = sqlite3.connect('z5291736.db')
        cur = con.cursor()
        cur.execute(
            'CREATE TABLE IF NOT EXISTS tvTable (id integer, tvmaze_id integer, name text, links_previous text, links_current text, links_next text, '
            'type text, language text, '
            'status text, runtime text, premiered text, officialSite text, weight integer, genres text, schedule_time text, schedule_days text, '
            'rating text, network_id integer, network_name text, network_country_name text, network_country_code text, network_country_timezone text, '
            'summary text, last_update text)')

        # Judge whether the ID entered by the user is valid
        print(id)
        if id < 1 or (isinstance(id, int) == False):
            # print("11111\n", type(id))
            return {'message': 'The id of this TV show is invalid.'}, 400

        # judge if tuple queried exists
        cur.execute(f"SELECT id FROM tvTable WHERE id = {id}")
        id_result = cur.fetchall()
        if id_result == []:
            return {'message': 'Cannot find a TV show whit this id.'}, 404
        else:
            cur.execute(f"SELECT id, tvmaze_id, name, links_previous, links_current, links_next, type, language, status, runtime, premiered, "
                        f"officialSite, weight, genres, schedule_time, schedule_days, rating, network_id, "
                        f"network_name, network_country_name, network_country_code, network_country_timezone, summary, last_update FROM tvTable WHERE id={id}")
            result = cur.fetchall()
            id = result[0][0]
            tvmaze_id = result[0][1]
            name = result[0][2]
            type = result[0][6]
            language = result[0][7]
            status = result[0][8]
            runtime = result[0][9]
            premiered = result[0][10]
            officialSite = result[0][11]
            weight = result[0][12]

            genres = result[0][13]
            genres_list = [] #the case that genres = 'None'
            if genres != "None" and "," not in genres:
                genres_list.append(genres)
            if genres != "None" and "," in genres:
                genres_list = genres.split(",")

            schedule_time = result[0][14]
            schedule_days = result[0][15]
            schedule_days_list = []  # the case that schedule_days = 'None'
            if schedule_days != "None" and "," not in schedule_days:  # the case that schedule_days = 'Saturday'
                schedule_days_list.append(schedule_days)
            if schedule_days != "None" and "," in schedule_days:  # the case that schedule_days = 'Friday, Sunday'
                schedule_days_list = schedule_days.split(",")

            rating = result[0][16]
            network_id = result[0][17]
            network_name = result[0][18]
            network_country_name = result[0][19]
            network_country_code = result[0][20]
            network_country_timezone = result[0][21]
            summary = result[0][22]
            last_update = result[0][23]

            cur.execute("SELECT id FROM tvTable WHERE id=?", (id - 1,))
            id_previous = cur.fetchall()
            cur.execute("SELECT id FROM tvTable WHERE id=?", (id + 1,))
            id_next = cur.fetchall()
            href_dict = dict()
            links_dict = dict()
            href_dict['href'] = f"http://127.0.0.1:5000/tv-shows/{id}"
            href_dict_tmp = copy.deepcopy(href_dict)
            links_dict['self'] = href_dict_tmp
            if id_previous != []:
                href_dict['href'] = f"http://127.0.0.1:5000/tv-shows/{id - 1}"
                href_dict_tmp = copy.deepcopy(href_dict)
                links_dict['previous'] = href_dict_tmp
            if id_next != []:
                href_dict['href'] = f"http://127.0.0.1:5000/tv-shows/{id + 1}"
                href_dict_tmp = copy.deepcopy(href_dict)
                links_dict['next'] = href_dict_tmp

            return {
                "tvmaze_id": tvmaze_id, "id": id, "last-update": last_update, "name": name, "type": type, "language": language,
                "genres": genres_list, "status": status, "runtime": runtime, "premiered": premiered, "officialSite": officialSite,
                "schedule": {"time": schedule_time, "days": schedule_days_list}, "rating": {"average": rating}, "weight": weight,
                "network": {"id": network_id, "name": network_name, "country": {"name": network_country_name, "code": network_country_code,
                                                                               "timezone": network_country_timezone}},
                "summary": summary, "_links": links_dict
            }, 200

    # question3
    @api.response(400, 'Invalid id')
    @api.response(404, 'id not found')
    @api.response(200, 'Deleted')
    def delete(self, id):
        # If the user executes the get command directly, there will be no table to query without creating a table
        con = sqlite3.connect('z5291736.db')
        cur = con.cursor()
        cur.execute(
            'CREATE TABLE IF NOT EXISTS tvTable (id integer, tvmaze_id integer, name text, links_previous text, links_current text, links_next text, '
            'type text, language text, '
            'status text, runtime text, premiered text, officialSite text, weight integer, genres text, schedule_time text, schedule_days text, '
            'rating text, network_id integer, network_name text, network_country_name text, network_country_code text, network_country_timezone text, '
            'summary text, last_update text)')

        # Judge whether the ID entered by the user is valid
        if id < 1 or isinstance(id, int) == False:
            # print("11111\n", type(id))
            return {'message': 'The id of this TV show is invalid.'}, 400

        # judge if tuple queried exists
        cur.execute(f"SELECT id FROM tvTable WHERE id = {id}")
        id_result = cur.fetchall()
        if id_result == []:
            return {'message': 'Cannot find a TV show with this id.'}, 404
        else:
            cur.execute(f'DELETE FROM tvTable WHERE id = {id}')
            con.commit()

            return {
                "message": f"The tv show with id {id} was removed from the database!",
                "id": id
            }, 200

    # Question4
    @api.response(400, 'Invalid id or invalid attributes')
    @api.response(404, 'id not found')
    @api.response(200, 'Updated')
    @api.expect(tv_payload)
    def patch(self, id):
        # If the user executes the get command directly, there will be no table to query without creating a table
        con = sqlite3.connect('z5291736.db')
        cur = con.cursor()
        cur.execute(
            'CREATE TABLE IF NOT EXISTS tvTable (id integer, tvmaze_id integer, name text, links_previous text, links_current text, links_next text, '
            'type text, language text, '
            'status text, runtime text, premiered text, officialSite text, weight integer, genres text, schedule_time text, schedule_days text, '
            'rating text, network_id integer, network_name text, network_country_name text, network_country_code text, network_country_timezone text, '
            'summary text, last_update text)')

        # Judge whether the ID entered by the user is valid
        if id < 1 or isinstance(id, int) == False:
            # print("11111\n", type(id))
            return {'message': 'The id of this TV show is invalid.'}, 400

        tv = request.json #取得payload并转换为json

        # judge if tuple queried exists
        cur.execute(f"SELECT id FROM tvTable WHERE id = {id}")
        id_result = cur.fetchall()
        if id_result == []:
            return {'message': 'Cannot find a TV show whit this id.'}, 404
        else:
            # update the value
            for key in tv:
                # unexpected key
                if key not in tv_payload.keys():
                    return {"message": "Attribute {} is invalid".format(key)}, 400
                if key == 'genres':
                    i = 1
                    genres_new_value = ''
                    for ele in tv[key]:
                        genres_new_value += ele
                        if i < len(tv[key]):
                            genres_new_value += ','
                        i += 1
                    cur.execute("UPDATE tvTable SET genres=? WHERE id=?", (genres_new_value,id))
                elif key == 'rating':
                    rating_new_value = tv[key]['average']
                    cur.execute("UPDATE tvTable SET rating=? WHERE id=?", (rating_new_value, id))
                elif key == 'schedule':
                    schedule_time_new_value = tv[key]['time']
                    cur.execute("UPDATE tvTable SET schedule_time=? WHERE id=?", (schedule_time_new_value, id))
                    i = 1
                    schedule_days_new_value = ''
                    for ele in tv[key]['days']:
                        schedule_days_new_value += ele
                        if i < len(tv[key]):
                            schedule_days_new_value += ','
                        i += 1
                    cur.execute("UPDATE tvTable SET schedule_days=? WHERE id=?", (schedule_days_new_value, id))
                elif key == 'network':
                    network_id_new_value = tv[key]['id']
                    cur.execute("UPDATE tvTable SET network_id=? WHERE id=?", (network_id_new_value, id))
                    network_name_new_value = tv[key]['name']
                    cur.execute("UPDATE tvTable SET network_name=? WHERE id=?", (network_name_new_value, id))
                    network_country_name_new_value = tv[key]['country']['name']
                    cur.execute("UPDATE tvTable SET network_country_name=? WHERE id=?", (network_country_name_new_value,id))
                    network_country_code_new_value = tv[key]['country']['code']
                    cur.execute("UPDATE tvTable SET network_country_code=? WHERE id=?", (network_country_code_new_value,id))
                    network_country_timezone_new_value = tv[key]['country']['timezone']
                    cur.execute("UPDATE tvTable SET network_country_timezone=? WHERE id=?", (network_country_timezone_new_value,id))
                else:
                    cur.execute(f"UPDATE tvTable set {key}='{tv[key]}' WHERE id = {id}")
            date = datetime.datetime.now()
            date_str = date.strftime("%Y-%m-%d %H:%M:%S")
            cur.execute("UPDATE tvTable SET last_update=? WHERE id=?", (date_str, id))
            con.commit()

            cur.execute("SELECT id FROM tvTable WHERE id=?", (id-1,))
            id_previous = cur.fetchall()
            cur.execute("SELECT id FROM tvTable WHERE id=?", (id+1,))
            id_next = cur.fetchall()
            href_dict = dict()
            links_dict = dict()
            href_dict['href'] = f"http://127.0.0.1:5000/tv-shows/{id}"
            href_dict_tmp = copy.deepcopy(href_dict)
            links_dict['self'] = href_dict_tmp
            if id_previous != []:
                href_dict['href'] = f"http://127.0.0.1:5000/tv-shows/{id-1}"
                href_dict_tmp = copy.deepcopy(href_dict)
                links_dict['previous'] = href_dict_tmp
            if id_next != []:
                href_dict['href'] = f"http://127.0.0.1:5000/tv-shows/{id+1}"
                href_dict_tmp = copy.deepcopy(href_dict)
                links_dict['next'] = href_dict_tmp

            return {
                "id": id,
                "last-update": date_str,
                "_links": links_dict
            }, 200

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
        # Get the parameters entered by the user, including order_ by,page, page, page_ size, filter
        order_by = request.args.get('order_by')
        page = request.args.get('page')
        page_size = request.args.get('page_size')
        filter = request.args.get('filter')

        # If the user executes the get command directly, there will be no table to query without creating a table
        con = sqlite3.connect('z5291736.db')
        cur = con.cursor()
        cur.execute(
            'CREATE TABLE IF NOT EXISTS tvTable (id integer, tvmaze_id integer, name text, links_previous text, links_current text, links_next text, '
            'type text, language text, '
            'status text, runtime text, premiered text, officialSite text, weight integer, genres text, schedule_time text, schedule_days text, '
            'rating text, network_id integer, network_name text, network_country_name text, network_country_code text, network_country_timezone text, '
            'summary text, last_update text)')

        # When the user does not enter a parameter, the default value is used
        if order_by == None:
            order_by = '+id'
        if page == None:
            page = '1'
        if page_size == None:
            page_size = '100'
        if filter == None:
            filter = 'id,name'

        # Process the order_by parameter
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

        order_by_list = order_by_no_space.split(',')
        filter_list = filter_no_space.split(',')

        # Check whether all input parameters are valid
        if page.isdigit() == False or page_size.isdigit() == False:
            return {'message': 'The page and page_size parameters can only be numeric.'}, 400
        standard_order_dy = ['+id', '-id', '+name', '-name', '+runtime', '-runtime', '+premiered', '-premiered',
                             '+rating-average', '-rating-average']
        standard_filter = ['tvmaze_id', 'id', 'last-update', 'name', 'type', 'language',
                           'genres', 'status', 'runtime', 'premiered', 'officialSite', 'schedule', 'rating',
                           'weight', 'network', 'summary']
        for i in order_by_list:
            if i not in standard_order_dy:
                return {'message': 'The order_by parameter is invalid.'}, 400

        for i in filter_list:
            if i not in standard_filter:
                return {'message': 'The filter parameter is invalid.'}, 400

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
        filter_query_list = filter_query_string.split(',')

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

        # Start query
        cur.execute(f"SELECT {filter_query_string} FROM tvTable ORDER BY {order_by_query_string}")
        result = cur.fetchall()
        if result == []:
            return {'message': 'There are no TV shows in the table at present.'}, 404
        else:
            total_item_num = len(result)
            current_page_num = 0
            required_page = ceil(total_item_num/int(page_size))

            tv_shows_dict = dict()
            tv_shows_list = []
            final_dict = dict()
            final_list = []
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
                    for j in range(real_page_size):
                        for attribute, ele in zip(filter_query_list, result[j + i * int(page_size)]):
                            if attribute == 'genres':
                                tv_shows_dict[attribute] = ele.split(',')
                            elif attribute == 'schedule_time':
                                schedule_dict["time"] = ele
                            elif attribute == 'schedule_days':
                                schedule_dict["days"] = ele.split(',')
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

                        tv_shows_dict_tmp = copy.deepcopy(tv_shows_dict)
                        tv_shows_list.append(tv_shows_dict_tmp)
                    final_dict["page"] = i + 1
                    final_dict["page-size"] = page_size
                    final_dict["tv-shows"] = tv_shows_list
                    if i == 0:
                        href_dict["href"] = f"http://127.0.0.1:5000/tv-shows?page={i+1},page_size={page_size}"
                        href_dict_tmp = copy.deepcopy(href_dict)
                        links_dict["self"] = href_dict_tmp
                        href_dict["href"] = f"http://127.0.0.1:5000/tv-shows?page={i+1+1},page_size={page_size}"
                        href_dict_tmp = copy.deepcopy(href_dict)
                        links_dict["next"] = href_dict_tmp
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
                    final_dict_tmp = copy.deepcopy(final_dict)
                    final_list.append(final_dict_tmp)
                    tv_shows_list = []
                    links_dict = dict()

                    current_page_num += 1
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

        # If the user executes the get command directly, there will be no table to query without creating a table
        con = sqlite3.connect('z5291736.db')
        cur = con.cursor()
        cur.execute(
            'CREATE TABLE IF NOT EXISTS tvTable (id integer, tvmaze_id integer, name text, links_previous text, links_current text, links_next text, '
            'type text, language text, '
            'status text, runtime text, premiered text, officialSite text, weight integer, genres text, schedule_time text, schedule_days text, '
            'rating text, network_id integer, network_name text, network_country_name text, network_country_code text, network_country_timezone text, '
            'summary text, last_update text)')

        # Judge whether the ID entered by the user is valid
        standard_format = ['json', 'image']
        standard_by = ['language', 'genres', 'status', 'type']
        if format not in standard_format:
            return {'message': 'The format parameter is invalid.'}, 400  #

        if by not in standard_by:
            return {'message': 'The by parameter is invalid.'}, 400  #

        cur.execute(f"SELECT id FROM tvTable")
        id_result = cur.fetchall()
        if id_result == []:
            return {'message': 'There are no TV shows in the table at present.'}, 404 #
        else:
            total_num = len(id_result) # total_num = total number of TV shows in database

        # get total num of TV shows updated in 24 hours
        last_date = datetime.datetime.today() - datetime.timedelta(days=1)
        last_date_str = last_date.strftime('%Y-%m-%d %H:%M:%S')

        cur.execute("SELECT id FROM tvTable WHERE last_update>=?", (last_date_str,))
        id_result = cur.fetchall()
        if id_result == []:
            # return {'message': 'No TV shows have been updated in the past 24 hours.'}, 404 #
            total_num_in_24 = 0
        else:
            total_num_in_24 = len(id_result)

        # get 'by' parameters
        if by == 'language' or by == 'status' or by == 'type':
            cur.execute(f"SELECT {by} FROM tvTable")
            result = cur.fetchall()
            result_list = []
            for i in result:
                result_list.append(i[0])
            count = dict(Counter(result_list))
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

        # json or image
        if format == 'json':
            return {
                "total": total_num,
                "total-updated": total_num_in_24,
                "values": count
            }, 200
        else:
            # Draw the first subgraph
            total_num_list_value = []
            total_num_list_key = []
            total_num_list_key.append("Total Num of TV shows")
            total_num_list_value.append(total_num)

            total_num_list_key.append("Total Num of TV shows (last 24 hours)")
            total_num_list_value.append(total_num_in_24)

            plt.figure(figsize=(8, 8), dpi=100)

            plt.subplot(2, 1, 1)
            plt.bar(total_num_list_key, total_num_list_value, align='center', width=0.1, alpha=0.8, color=('r', 'b'))
            plt.ylabel("amount")
            plt.title("Total num and total num in last 24 hours", loc='left', fontsize=12, fontweight='semibold')

            # Draw the second subgraph(pie chart)
            plt.subplot(2, 1, 2)
            label = list(count.keys())
            data = list(count.values())
            plt.pie(data, labels=label, radius=0.5, autopct='%3.2f%%')
            plt.axis('equal')
            plt.title(f"Percentage of TV shows per {by}", loc='left', fontsize=15, fontweight='semibold')
            plt.legend(loc='upper right', bbox_to_anchor=(1.1, 1.05), fontsize=10, borderaxespad=0.3)

            plt.savefig("question6.png")
            return send_file("question6.png")



if __name__ == '__main__':
    app.run(debug=True)