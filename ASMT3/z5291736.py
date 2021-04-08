import numpy as np
import pandas as pd
import sys
import json
from scipy import stats
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, average_precision_score, accuracy_score, recall_score

# def extract_id_or_iso(column, id_or_iso, feature):
def extract_id_or_iso(column, id_or_iso):
    # df = column.str.extractall('"id":\d+')
    # print(df)
    resultList = []
    for ele in column:
        resultString = ''
        row = json.loads(ele)
        # sizeOfRow = len(row)
        # count = 1
        count = 0
        for dict in row:
            if id_or_iso == 'id':
                if 'id' in dict.keys():
                    count += 1
            else:
                if 'iso_3166_1' in dict.keys():
                    count += 1
        resultList.append(count)
    # print(idList)
    return resultList

def process_data(df):
    # process column of release_date
    df['release_date'] = df['release_date'].str.extract('^(\d{4})', expand=False).apply(pd.to_numeric)

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
    # df.to_csv(path_or_buf="df1.csv", index=False)
    return df

def output_file(df, prediction, partNum):
    if partNum == 'PART1':
        movieIdList = df['movie_id'].values.tolist()
        predList = [int(round(pred, 0)) for pred in prediction]
        outputDict = {'movie_id': movieIdList, 'predicted_revenue': predList}
        outputDf = pd.DataFrame(outputDict)
        outputDf.to_csv(path_or_buf=f"z5291736.{partNum}.output.csv", index=False)

def summary_file(y_valid, prediction, partNum):
    if partNum == 'PART1':
        MSR = round(mean_squared_error(y_valid, prediction), 2)
        coef, p_value = stats.pearsonr(y_valid, prediction)
        outputDict = {'zid': ['z5291736'], 'MSR': [MSR], 'correlation': [round(coef, 2)]}
        outputDf = pd.DataFrame(outputDict)
        outputDf.to_csv(path_or_buf=f"z5291736.{partNum}.summary.csv", index=False)

def part1(trainDf, validDf):
    dfTmp1 = trainDf.loc[:, ['movie_id', 'revenue']]
    trainDf = process_data(trainDf)
    dfTmp2 = validDf.loc[:, ['movie_id', 'revenue']]
    validDf = process_data(validDf)

    X_train = trainDf.values
    y_train = dfTmp1['revenue'].values
    X_valid = validDf.values
    y_valid = dfTmp2['revenue'].values
    # trainDf.to_csv(path_or_buf="5291.csv", index=False)
    model = RandomForestRegressor(n_estimators=125, max_depth=8)
    model_fit = model.fit(X_train, y_train)
    prediction = model_fit.predict(X_valid) # prediction是一个list
    # print(len(prediction))

    # generate output.csv
    output_file(dfTmp2, prediction, 'PART1')
    summary_file(y_valid, prediction, 'PART1')






if __name__ == "__main__":
    trainingPath = 'training.csv'
    validationPath = 'validation.csv'
    # trainingPath, validationPath = sys.argv[1], sys.argv[2]
    trainDf = pd.read_csv(trainingPath)
    validDf = pd.read_csv(validationPath)
    part1(trainDf, validDf)
    # part2(trainPath, validationPath)