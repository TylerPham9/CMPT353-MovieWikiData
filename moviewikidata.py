import pandas as pd
import numpy as np
from sklearn.svm import SVR
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MultiLabelBinarizer
from scipy import stats
from statsmodels.stats.multicomp import pairwise_tukeyhsd
import data.data as dm
import os.path
import argparse

def main():

    category = 'genre'
    score = 'return'
    min_num_of_movies = 40
    num_of_influencers = 25
    filename = '{}-{}-{}-{}'.format(category, score, min_num_of_movies,
                                    num_of_influencers)

    if os.path.isfile(dm.JSON_PATH.format(filename)):
        data = dm.json_to_df(filename)
    else:
        data = dm.get_filtered_wikidata('wikidata-movies', category, score,
                                        min_num_of_movies, num_of_influencers)
        data = dm.explode_dataframe_by_column(data, category)
        data = dm.map_wikidata_id(data, category)
        dm.df_to_json(data, filename)


    print("DEGUG: TukeyHSD")
    data_pivoted = data.pivot(columns=category)[score]
    new_data = [data_pivoted[col].dropna() for col in data_pivoted]
    f_val, p_val = stats.f_oneway(*new_data)
    print(p_val)
    posthoc = pairwise_tukeyhsd(
        data[score], data[category],
        alpha=0.05)

    fig = posthoc.plot_simultaneous()
    fig.show()
    # interesting_values = json_to_df('interesting_values')
    # wikidata_df = json_to_df('wikidata-movies')
    # df = json_to_df('movies_by_return')

    # mlb = MultiLabelBinarizer()
    # X = pd.DataFrame(mlb.fit_transform(df['poi']), columns=mlb.classes_,
    #                  index=df.index)
    # y = df[score]
    # X_train, X_test, y_train, y_test = train_test_split(X, y)
    #
    # model = SVR()
    # model.fit(X_train, y_train)
    # print(model.score(X_test, y_test))

    print("Done!")


if __name__ == '__main__':
    main()
