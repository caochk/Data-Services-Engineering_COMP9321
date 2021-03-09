import json
import matplotlib.pyplot as plt
import pandas as pd
import sys
import os
import numpy as np
import math

studentid = os.path.basename(sys.modules[__name__].__file__)


def log(question, output_df, other):
    print("--------------- {}----------------".format(question))

    if other is not None:
        print(question, other)
    if output_df is not None:
        df = output_df.head(5).copy(True)
        for c in df.columns:
            df[c] = df[c].apply(lambda a: a[:20] if isinstance(a, str) else a)

        df.columns = [a[:10] + "..." for a in df.columns]
        print(df.to_string())


def question_1(exposure, countries):
    """
    :param exposure: the path for the exposure.csv file
    :param countries: the path for the Countries.csv file
    :return: df1
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    df_exposureTemp = pd.read_csv(exposure, sep=';', encoding='latin-1')
    # df_exposure = df_exposureTemp[~df_exposureTemp.isna().any(axis=1)]
    df_exposure = df_exposureTemp.dropna(subset=['country'])
    df_countriesTemp = pd.read_csv(countries)
    df_countries = df_countriesTemp.rename(str.lower, axis='columns')
    df2 = pd.merge(df_exposure, df_countries, how='inner', on='country')
    df3 = df2.rename(columns={'country': 'Country'})
    df4 = df3.set_index('Country')
    df5 = df4.sort_index()
    df1 = df5
    #################################################

    log("QUESTION 1", output_df=df1, other=df1.shape)
    return df1


def question_2(df1):
    """
    :param df1: the dataframe created in question 1
    :return: df2
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    pd.set_option('display.width', 100) # to be deleted
    pd.set_option('max_colwidth', 1000000) # to be deleted
    pd.set_option('display.max_rows', 300) # to be deleted

    dfLatitudeTemp = df1['cities'].str.extractall('(?P<latitude>"Latitude":-?\d+.\d+)')
    dfLatitude = dfLatitudeTemp['latitude'].str.extractall('(?P<latitude>-?\d+.\d+)')
    dfLatitude["latitude"] = pd.to_numeric(dfLatitude["latitude"], errors='coerce')
    dfAvgLatitude = dfLatitude.groupby('Country')['latitude'].mean()


    dfLongitudeTemp = df1['cities'].str.extractall('(?P<longitude>"Longitude":-?\d+.\d+)')
    dfLongitude = dfLongitudeTemp['longitude'].str.extractall('(?P<longitude>-?\d+.\d+)')
    dfLongitude["longitude"] = pd.to_numeric(dfLongitude["longitude"], errors='coerce')
    dfAvgLongitude = dfLongitude.groupby('Country')['longitude'].mean()

    df2Temp = df1.join(dfAvgLatitude)
    df2 = df2Temp.join(dfAvgLongitude)

    df2 = df2.rename(columns={'latitude': 'avg_latitude', 'longitude': 'avg_longitude'})
    #################################################

    log("QUESTION 2", output_df=df2[["avg_latitude", "avg_longitude"]], other=df2.shape)
    return df2


def question_3(df2):
    """
    :param df2: the dataframe created in question 2
    :return: df3
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    df2['latitudeDifference'] = df2['avg_latitude'].apply(lambda x: math.radians(x) - math.radians(30.5928))
    df2['longitudeDifference'] = df2['avg_longitude'].apply(lambda x: math.radians(x) - math.radians(114.3055))
    print(df2.dtypes)
    df2['distance_to_Wuhan'] = df2.loc[:, ['latitudeDifference', 'longitudeDifference', 'avg_latitude']].apply(lambda x: 6373 * (2 * math.asin(math.sqrt(math.pow(math.sin(x.iloc[0] / 2), 2) + math.cos(math.radians(x.iloc[2])) * math.cos(math.radians(30.5928)) * math.pow(math.sin(x.iloc[1] / 2), 2)))), axis = 1)
    df3 = df2.sort_values(by=['distance_to_Wuhan'])
    df3.drop(['latitudeDifference', 'longitudeDifference'], inplace=True, axis=1)
    #################################################

    log("QUESTION 3", output_df=df3[['distance_to_Wuhan']], other=df3.shape)
    return df3


def question_4(df2, continents):
    """
    :param df2: the dataframe created in question 2
    :param continents: the path for the Countries-Continents.csv file
    :return: df4
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    df_countriesContinents = pd.read_csv('Countries-Continents.csv')
    df_countriesContinents.set_index('Country', inplace=True)
    df4_temp = df2.join(df_countriesContinents)
    df4_temp['Covid_19_Economic_exposure_index'].replace("x", "0,0", inplace=True)
    df4_temp['Covid_19_Economic_exposure_index'].replace("(\d+),(\d+)", r"\1.\2", inplace=True, regex=True)
    df4_temp["Covid_19_Economic_exposure_index"] = pd.to_numeric(df4_temp["Covid_19_Economic_exposure_index"], errors='coerce')
    # print("------------------START--------------------")
    df4_temp2 = df4_temp.groupby('Continent')['Covid_19_Economic_exposure_index'].mean()
    df4_temp3 = df4_temp2.reset_index()
    df4_temp4 = df4_temp3.set_index('Continent', drop=False)
    df4 = df4_temp4.sort_values(by=['Covid_19_Economic_exposure_index'])
    # print(df4)
    # print(df4_temp2)
    # df4_temp.to_csv(path_or_buf="df4Temp.csv", index=False)
    #################################################

    log("QUESTION 4", output_df=df4, other=df4.shape)
    return df4


def question_5(df2):
    """
    :param df2: the dataframe created in question 2
    :return: cities_lst
            Data Type: list
            Please read the assignment specs to know how to create the output dataframe
    """
    #################################################
    # Your code goes here ...
    #################################################

    log("QUESTION 5", output_df=df5, other=df5.shape)
    return df5


def question_6(df2):
    """
    :param df2: the dataframe created in question 2
    :return: lst
            Data Type: list
            Please read the assignment specs to know how to create the output dataframe
    """
    cities_lst = []
    #################################################
    # Your code goes here ...
    #################################################

    log("QUESTION 6", output_df=None, other=cities_lst)
    return lst


def question_7(df2):
    """
    :param df2: the dataframe created in question 2
    :return: df7
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    #################################################

    log("QUESTION 7", output_df=df7, other=df7.shape)
    return df7


def question_8(df2, continents):
    """
    :param df2: the dataframe created in question 2
    :param continents: the path for the Countries-Continents.csv file
    :return: nothing, but saves the figure on the disk
    """

    #################################################
    # Your code goes here ...
    #################################################

    plt.savefig("{}-Q11.png".format(studentid))


def question_9(df2):
    """
    :param df2: the dataframe created in question 2
    :return: nothing, but saves the figure on the disk
    """

    #################################################
    # Your code goes here ...
    #################################################

    plt.savefig("{}-Q12.png".format(studentid))


def question_10(df2, continents):
    """
    :param df2: the dataframe created in question 2
    :return: nothing, but saves the figure on the disk
    :param continents: the path for the Countries-Continents.csv file
    """

    #################################################
    # Your code goes here ...
    #################################################

    plt.savefig("{}-Q13.png".format(studentid))


if __name__ == "__main__":
    df1 = question_1("exposure.csv", "Countries.csv")
    df2 = question_2(df1.copy(True))
    df3 = question_3(df2.copy(True))
    df4 = question_4(df2.copy(True), "Countries-Continents.csv")
    df5 = question_5(df2.copy(True))
    lst = question_6(df2.copy(True))
    df7 = question_7(df2.copy(True))
    question_8(df2.copy(True), "Countries-Continents.csv")
    question_9(df2.copy(True))
    question_10(df2.copy(True), "Countries-Continents.csv")