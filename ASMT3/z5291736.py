import numpy as np
import pandas as pd
import sys
import json

# def extract_id_or_iso(column, id_or_iso, feature):
def extract_id_or_iso(column, id_or_iso):
    # df = column.str.extractall('"id":\d+')
    # print(df)
    resultList = []
    for ele in column:
        resultString = ''
        row = json.loads(ele)
        sizeOfRow = len(row)
        count = 1
        for dict in row:
            if id_or_iso == 'id':
                if 'id' in dict.keys():
                    resultString += str(dict['id'])
                if count < sizeOfRow:
                    resultString += ','
                    count += 1
            else:
                if 'iso_3166_1' in dict.keys():
                    resultString += str(dict['iso_3166_1'])
                if count < sizeOfRow:
                    resultString += ','
                    count += 1
        resultList.append(resultString)
    # print(idList)
    return resultList


def process_data(df):
    # process column of release_date
    df['release_date'] = df['release_date'].str.extract('^(\d{4})', expand=False)

    # process column of cast, crew, genres, keywords and production_companies
    df['cast'] = extract_id_or_iso(df['cast'], 'id')
    df['crew'] = extract_id_or_iso(df['crew'], 'id')
    df['genres'] = extract_id_or_iso(df['genres'], 'id')
    df['keywords'] = extract_id_or_iso(df['keywords'], 'id')
    df['production_companies'] = extract_id_or_iso(df['production_companies'], 'id')
    # process column of production_countries
    df['production_countries'] = extract_id_or_iso(df['production_countries'], 'iso')

    # df.to_csv(path_or_buf="df.csv", index=False)
    # drop some features that we doesn't need
    df.drop(columns=['movie_id', 'homepage', 'original_title', 'original_language', 'overview', 'revenue',
                     'spoken_languages', 'status', 'tagline', 'rating'], inplace=True)


def part1(trainDf, validDf):
    process_data(trainDf)






if __name__ == "__main__":
    trainingPath = 'training.csv'
    validationPath = 'validation.csv'
    # trainingPath, validationPath = sys.argv[1], sys.argv[2]
    trainDf = pd.read_csv(trainingPath)
    validDf = pd.read_csv(validationPath)
    part1(trainDf, validDf)
    # part2(trainPath, validationPath)