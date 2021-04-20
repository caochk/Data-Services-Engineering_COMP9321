import numpy as np
import pandas as pd
import sys
import json
from scipy import stats
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, accuracy_score, recall_score, average_precision_score
from sklearn.neighbors import KNeighborsClassifier

# def extract_id_or_iso(column, id_or_iso, feature):
def extract_id_or_iso(column, id_or_iso):
    resultList = []
    for ele in column:
        resultString = ''
        row = json.loads(ele)
        count = 0
        for dict in row:
            if id_or_iso == 'id':
                if 'id' in dict.keys():
                    count += 1
            else:
                if 'iso_3166_1' in dict.keys():
                    count += 1
        resultList.append(count)
    return resultList

def process_data(df):
    # process column of release_date
    df['release_date'] = df['release_date'].str.extract('^(\d{4})', expand=False).apply(pd.to_numeric)

    # process column of cast, crew, genres, keywords and production_companies
    df['cast'] = extract_id_or_iso(df['cast'], 'id')
    df['crew'] = extract_id_or_iso(df['crew'], 'id')
    df['genres'] = extract_id_or_iso(df['genres'], 'id')
    df = df.replace(np.nan, 0)
    df['homepage'] = df['homepage'].apply(lambda x: 1 if x != 0 else 0)
    df['keywords'] = extract_id_or_iso(df['keywords'], 'id')
    df['production_companies'] = extract_id_or_iso(df['production_companies'], 'id')
    # process column of production_countries
    df['production_countries'] = extract_id_or_iso(df['production_countries'], 'iso')

    # drop some features that we don't need
    df.drop(columns=['movie_id', 'original_title', 'original_language', 'overview', 'revenue', 'cast', 'crew',
                     'keywords', 'production_countries',
                     'spoken_languages',  'status', 'tagline', 'rating'], inplace=True)
    return df

def output_file(df, prediction, partNum):
    if partNum == 'PART1':
        movieIdList = df['movie_id'].values.tolist()
        predList = [int(round(pred, 0)) for pred in prediction]
        outputDict = {'movie_id': movieIdList, 'predicted_revenue': predList}
        outputDf = pd.DataFrame(outputDict)
        outputDf.to_csv(path_or_buf=f"z5291736.{partNum}.output.csv", index=False)
    else:
        movieIdList = df['movie_id'].values.tolist()
        predList = [int(round(pred, 0)) for pred in prediction]
        outputDict = {'movie_id': movieIdList, 'predicted_rating': predList}
        outputDf = pd.DataFrame(outputDict)
        outputDf.to_csv(path_or_buf=f"z5291736.{partNum}.output.csv", index=False)

def summary_file(y_valid, prediction, partNum):
    if partNum == 'PART1':
        MSR = round(mean_squared_error(y_valid, prediction), 2)
        r, p_value = stats.pearsonr(y_valid, prediction)
        outputDict = {'zid': ['z5291736'], 'MSR': [MSR], 'correlation': [round(r, 2)]}
        outputDf = pd.DataFrame(outputDict)
        outputDf.to_csv(path_or_buf=f"z5291736.{partNum}.summary.csv", index=False)
    else:
        avgPrecision = np.around([average_precision_score(y_valid, prediction, average='macro', pos_label=3)], 2)
        avgRecall = np.around([recall_score(y_valid, prediction, average='macro')], 2)
        accuracy = np.around([accuracy_score(y_valid, prediction)], 2)
        outputDict = {'zid': ['z5291736'], 'average_precision': avgPrecision, 'average_recall': avgRecall,
                      'accuracy': accuracy}
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

    model = RandomForestRegressor(n_estimators=120, max_depth=8)
    model_fit = model.fit(X_train, y_train)
    prediction = model_fit.predict(X_valid)

    # generate output.csv and summary.csv
    output_file(dfTmp2, prediction, 'PART1')
    summary_file(y_valid, prediction, 'PART1')

def part2(trainDf, validDf):
    dfTmp1 = trainDf.loc[:, ['movie_id', 'rating']]
    trainDf = process_data(trainDf)
    dfTmp2 = validDf.loc[:, ['movie_id', 'rating']]
    validDf = process_data(validDf)

    X_train = trainDf.values
    y_train = dfTmp1['rating'].values
    X_valid = validDf.values
    y_valid = dfTmp2['rating'].values

    model = KNeighborsClassifier()
    model_fit = model.fit(X_train, y_train)
    prediction = model_fit.predict(X_valid)

    # generate output.csv and summary.csv
    output_file(dfTmp2, prediction, 'PART2')
    summary_file(y_valid, prediction, 'PART2')




if __name__ == "__main__":
    trainingPath, validationPath = sys.argv[1], sys.argv[2]
    trainDf = pd.read_csv(trainingPath)
    validDf = pd.read_csv(validationPath)
    part1(trainDf, validDf)
    trainDf = pd.read_csv(trainingPath)
    validDf = pd.read_csv(validationPath)
    part2(trainDf, validDf)