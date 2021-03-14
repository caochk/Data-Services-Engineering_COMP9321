import json
import matplotlib.pyplot as plt
import pandas as pd
import sys
import os
import numpy as np
import math
import re

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
    df_exposureTemp = pd.read_csv(exposure, sep=';', encoding='latin-1', low_memory=False)
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
    # print(df2.dtypes)
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
    df_countriesContinents = pd.read_csv(continents)
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
    :return: df5
            Data Type: dataframe
            Please read the assignment specs to know how to create the output dataframe
    """
    #################################################
    df5_temp1 = df2.loc[:, ['Income classification according to WB', 'Foreign direct investment', 'Net_ODA_received_perc_of_GNI']]
    df5_temp2 = df5_temp1.drop(df5_temp1[df5_temp1.Net_ODA_received_perc_of_GNI == 'No data'].index)
    df5_temp3 = df5_temp2.drop(df5_temp2[df5_temp2['Foreign direct investment'] == 'x'].index)
    df5_temp3['Net_ODA_received_perc_of_GNI'].replace("(\d+),(\d+)", r"\1.\2", inplace=True, regex=True)
    df5_temp3['Foreign direct investment'].replace("(\d+),(\d+)", r"\1.\2", inplace=True, regex=True)
    df5_temp3["Net_ODA_received_perc_of_GNI"] = pd.to_numeric(df5_temp3["Net_ODA_received_perc_of_GNI"], errors='coerce')
    df5_temp3["Foreign direct investment"] = pd.to_numeric(df5_temp3["Foreign direct investment"], errors='coerce')
    df5_temp4 = df5_temp3.groupby('Income classification according to WB').mean()
    df5_temp4 = df5_temp4.reset_index()
    df5_temp5 = df5_temp4.rename(columns={'Income classification according to WB': 'Income Class', 'Foreign direct investment': 'Avg Foreign direct investment', 'Net_ODA_received_perc_of_GNI': 'Avg_Net_ODA_received_perc_of_GNI'})
    df5 = df5_temp5.set_index('Income Class', drop=False)
    # print(df5_temp4)
    # print(df5_temp3.dtypes)
    # df5.to_csv(path_or_buf="df5.csv", index=False)
    #################################################

    log("QUESTION 5", output_df=df5, other=df5.shape)
    return df5


def question_6(df2):
    """
    :param df2: the dataframe created in question 2
    :return: cities_lst
            Data Type: list
            Please read the assignment specs to know how to create the output dataframe
    """
    cities_lst = []
    #################################################
    # print("----------------START--------------------")
    df6_temp1 = df2[df2['Income classification according to WB']=='LIC']
    # print(df6_temp1)
    # df6_temp1.to_csv(path_or_buf="df6Temp1.csv", index=False)
    df6_temp2 = df6_temp1['cities']
    df6_temp3 = df6_temp2.str.extractall('"City":"(\w+)"')+df6_temp2.str.extractall('"Population"(:\d+.\d+)')
    # df6_temp4 = df6_temp3.iloc[:, 0]
    df6_temp4 = df6_temp3.iloc[:, 0].str.split(':', expand=True)
    df6_temp5 = df6_temp4.reset_index()
    df6_temp5.iloc[:, 3] = pd.to_numeric(df6_temp5.iloc[:, 3], errors='coerce')
    df6_temp5.columns = ["Country", "match", "city", "population"] #给没有列名的列添加名字
    df6_temp6 = df6_temp5.sort_values(by=['population'], ascending=False)
    df6 = df6_temp6.iloc[:5, 2]
    cities_lst = df6.values.tolist()
    lst = cities_lst
    # df6_temp6 = df6_temp5.sort_value(by)
    # print(df6)
    # df6_temp4.to_csv(path_or_buf="df6temp7.csv", index=False)
    # print("--------------------END----------------------")
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
    print("--------7--------")
    df7_temp1 = df2['cities'].str.extractall('"City":"(\w+)"')
    df7_temp2 = df7_temp1.reset_index()
    df7_temp2.columns = ["Country", "match", "city"]
    df7_temp3 = df7_temp2['city'].value_counts()
    # df7_temp3.reset_index()
    # df7_temp3.columns = ["city", "amount"]
    df7_temp4 = pd.DataFrame({'city': df7_temp3.index, 'amount': df7_temp3.values})
    df7_temp5 = df7_temp2.merge(df7_temp4, left_on='city', right_on='city', how='inner')
    # df7_temp4 = df7_temp2.groupby('Country')
    df7_temp6 = df7_temp5.drop(columns=['match'])
    df7_temp7 = df7_temp6.drop_duplicates(keep=False)
    df7_temp8 = df7_temp7.drop(df7_temp7[df7_temp7.amount < 2].index)

    df7_temp9 = df7_temp8['city'].value_counts()
    df7_temp10 = pd.DataFrame({'city': df7_temp9.index, 'amount': df7_temp9.values})
    df7_temp11 = df7_temp10.merge(df7_temp2, left_on='city', right_on='city', how='inner')
    df7_temp12 = df7_temp11.drop(columns=['match'])
    df7_temp13 = df7_temp12.drop_duplicates(keep=False)
    df7_temp14 = df7_temp13.drop(df7_temp13[df7_temp13.amount < 2].index)

    df7_temp15 = df7_temp14.groupby(['city']).agg(country=('Country', lambda x: ','.join(x)))
    df7_temp16 = df7_temp15.reset_index()
    df7 = df7_temp16.set_index('city')
    # print(df7_temp16)
    print("---------7---------")
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
    # print("-------8------")
    df2_1 = df2.reset_index()
    # print(df2_1)
    df_continents = pd.read_csv(continents)
    # print(continents)
    df8_temp1 = df2_1.merge(df_continents, left_on='Country', right_on='Country', how='inner')
    df8_temp2 = df8_temp1['cities']
    df8_temp3 = df8_temp2.str.extractall('"Population":(\d+.\d+)')
    # df8_temp2.to_csv(path_or_buf="df8Temp2.csv", index=False)
    df8_temp3.columns = ["population"]
    df8_temp3["population"] = pd.to_numeric(df8_temp3["population"], errors='coerce')
    worldPopulation = df8_temp3['population'].sum()
    df8_temp4 = df8_temp1[df8_temp1['Continent'] == 'South America']
    df8_temp5 = df8_temp4.set_index('Country')
    df8_temp6 = df8_temp5['cities'].str.extractall('"Population":(\d+.\d+)')
    df8_temp6.columns = ["population"]
    df8_temp6["population"] = pd.to_numeric(df8_temp6["population"], errors='coerce')
    df8_temp6['percentageOfPopulation'] = df8_temp6['population'].apply(lambda x: x/worldPopulation)
    df8_temp7 = df8_temp6.reset_index()
    df8_temp8 = df8_temp7.groupby('Country')['percentageOfPopulation'].sum()
    # df8_temp9 = df8_temp8.reset_index()
    # df8_temp8.to_csv(path_or_buf="df8temp8.csv", index=False)
    # df8_temp8.plot.pie(y='percentageOfPopulation', autopct='%.4f')
    df8_temp8.plot(kind='bar', figsize=(16,10))
    # plt.show()
    # print(df8_temp9.dtypes)
    # print("-------8------")
    #################################################

    plt.savefig("{}-Q11.png".format(studentid))


def question_9(df2):
    """
    :param df2: the dataframe created in question 2
    :return: nothing, but saves the figure on the disk
    """

    #################################################
    # print("-----------9-----") #再自己加一个copy函数可以消除吗？
    df9_temp1 = df2.iloc[:, [1, 19, 20, 16, 17]]
    df9_temp1.loc[:, "Foreign direct investment, net inflows percent of GDP"].replace("x", "0,0", inplace=True)
    df9_temp1.loc[:, "Foreign direct investment"].replace("x", "0,0", inplace=True)
    df9_temp1.loc[:, 'Covid_19_Economic_exposure_index_Ex_aid_and_FDI'].replace("(\d+),(\d+)", r"\1.\2", inplace=True, regex=True)
    df9_temp1.loc[:, 'Covid_19_Economic_exposure_index_Ex_aid_and_FDI_and_food_import'].replace("(\d+),(\d+)", r"\1.\2", inplace=True, regex=True)
    df9_temp1.loc[:, 'Foreign direct investment, net inflows percent of GDP'].replace("(\d+),(\d+)", r"\1.\2", inplace=True, regex=True)
    df9_temp1.loc[:, 'Foreign direct investment'].replace("(\d+),(\d+)", r"\1.\2", inplace=True, regex=True)

    df9_temp1.loc[:, "Covid_19_Economic_exposure_index_Ex_aid_and_FDI"] = pd.to_numeric(df9_temp1.loc[:, "Covid_19_Economic_exposure_index_Ex_aid_and_FDI"], errors='coerce')
    df9_temp1.loc[:, "Covid_19_Economic_exposure_index_Ex_aid_and_FDI_and_food_import"] = pd.to_numeric(df9_temp1.loc[:, "Covid_19_Economic_exposure_index_Ex_aid_and_FDI_and_food_import"], errors='coerce')
    df9_temp1.loc[:, "Foreign direct investment, net inflows percent of GDP"] = pd.to_numeric(df9_temp1.loc[:, "Foreign direct investment, net inflows percent of GDP"], errors='coerce')
    df9_temp1.loc[:, "Foreign direct investment"] = pd.to_numeric(df9_temp1.loc[:, "Foreign direct investment"], errors='coerce')

    # df9_temp2 = pd.to_numeric(df9_temp1.loc[:, "Covid_19_Economic_exposure_index_Ex_aid_and_FDI"], errors='coerce')
    # df9_temp2 = pd.to_numeric(df9_temp1.loc[:, "Covid_19_Economic_exposure_index_Ex_aid_and_FDI_and_food_import"], errors='coerce')
    # df9_temp2 = pd.to_numeric(df9_temp1.loc[:, "Foreign direct investment, net inflows percent of GDP"], errors='coerce')
    # df9_temp2 = pd.to_numeric(df9_temp1.loc[:, "Foreign direct investment"], errors='coerce')
    # #
    df9_temp2 = df9_temp1.groupby('Income classification according to WB').mean()
    df9_temp3 = df9_temp2.T
    #
    df9_temp3.plot(kind='bar', figsize=(16, 16))
    plt.xticks(rotation=50, fontsize=13)
    # plt.show()
    # print(df9_temp1)
    # df9_temp1.to_csv(path_or_buf="df9temp1.csv", index=False)
    # print("----------9-------")
    #################################################

    plt.savefig("{}-Q12.png".format(studentid))


def question_10(df2, continents):
    """
    :param df2: the dataframe created in question 2
    :return: nothing, but saves the figure on the disk
    :param continents: the path for the Countries-Continents.csv file
    """

    #################################################
    # print("--------10----------")
    df2_1 = df2.reset_index()
    df_continents = pd.read_csv(continents)
    df10_temp1 = df2_1.merge(df_continents, left_on='Country', right_on='Country', how='inner')
    df10_temp2 = df10_temp1.set_index('Country')
    df10_temp3 = df10_temp2['cities']
    df10_temp4 = df10_temp3.str.extractall('"Population":(\d+.\d+)')
    df10_temp4.columns = ['population']
    df10_temp4["population"] = pd.to_numeric(df10_temp4["population"], errors='coerce')
    df10_temp5 = df10_temp4.groupby('Country')['population'].sum()
    df10_temp5.reset_index()
    df10_temp6 = pd.merge(df10_temp5, df10_temp1, how='inner', on='Country')
    df10_temp7 = df10_temp6.iloc[:, [0, 1, 24, 25, 26]]

    sizeOfCountries = (df10_temp7['population']/650000).values.tolist()#按照人口画国家大小

    df10_temp7.insert(loc=5, column='colour', value=0)
    df_colour = df10_temp7.loc[:, ['Continent', 'colour']]
    df_colour.loc[df_colour.Continent == 'Asia', 'colour'] = 'r'
    df_colour.loc[df_colour.Continent == 'Europe', 'colour'] = 'm'
    df_colour.loc[df_colour.Continent == 'Africa', 'colour'] = 'y'
    df_colour.loc[df_colour.Continent == 'North America', 'colour'] = 'b'
    df_colour.loc[df_colour.Continent == 'Oceania', 'colour'] = 'c'
    df_colour.loc[df_colour.Continent == 'South America', 'colour'] = 'g'
    colours = df_colour.loc[:, 'colour'].tolist()

    continent = df_colour.loc[:, 'Continent'].tolist()

    df10_temp7.plot.scatter(x='avg_longitude', y='avg_latitude', s=sizeOfCountries, c=colours)
    plt.legend(continent)
    # plt.show()
    # print(df_colour)
    # df_colour.to_csv(path_or_buf="df_colour.csv", index=False)
    # print("-----------10-------------")
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
    # question_9(df2.copy(True))
    question_10(df2.copy(True), "Countries-Continents.csv")