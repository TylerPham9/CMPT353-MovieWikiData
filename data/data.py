import sys
import pandas as pd
import numpy as np
import gzip
from os import path
from datetime import datetime

JSON_PATH = path.dirname(__file__) + '/json/{}.json.gz'


def json_to_df(file):
    """
    opens the gzip file into a pd dataframe
    :param file: string, name of the file
    :return: dataframe of the json file
    """
    data = pd.read_json(gzip.open(path.abspath(JSON_PATH.format(file)), 'rt',
                                  encoding='utf-8'), lines=True)
    return data


def df_to_json(df, file):
    """
    saves pd dataframe as gzip file
    :param df: Dataframe
    :param file: String, name of the gzip file
    """
    df.to_json(path.abspath(JSON_PATH.format(file)),
               orient='records',
               lines=True,
               compression='gzip')


def prep_wikidata(wikidata_df, category, rating, year):
    """
    Remove extra columns of the wikidata dataframe and only keep a specific
    category ('cast_member', 'director', 'genre'), 'label', 'publication_date',
    'return' (nbox/ncost), and 'wikidata_id'. Remove any NaN in category
    :param wikidata_df: Dataframe, wikidata dataframe
    :param category: String, column of interest
    :param rating: String, type of rating to focus
    :return: dataframe with extra columns removed
    """
    columns = [category, 'label', 'publication_date', 'wikidata_id',
               'rotten_tomatoes_id']
    if rating == 'return':
        columns += ['return']
    wikidata_df = wikidata_df[columns]
    wikidata_df = wikidata_df.dropna(subset=[category])


    if rating == 'return':
        wikidata_df = clean_return_data(wikidata_df)
    # audience_average, audience_percent, audience_ratings, critic_average,
    # critic_percent
    else:
        wikidata_df = merge_rt_data(wikidata_df, rating)

    if year:
        wikidata_df = wikidata_df[pd.notnull(wikidata_df['publication_date'])]
        wikidata_df['year'] = wikidata_df['publication_date'].apply(get_year)
        wikidata_df = wikidata_df .loc[(wikidata_df['year'] >= year[0]) &
                                       (wikidata_df['year'] < year[1])]
    return wikidata_df


def get_year(date):
    year = datetime.strptime(date, '%Y-%m-%d').year
    return year


def clean_return_data(data):
    """
    removes all rows without values in "return" and values deemed too small
    or too large
    :param data: dataframe, dataframe to be cleaned
    :return: dataframe
    """
    data = data[pd.notnull(data['return'])]
    data = data.loc[(data['return'] >= 0.0001) &
                    (data['return'] <= 3000)]
    return data.sort_values('return', axis=0,
                            ascending=False)


def merge_rt_data(wikidata_df, rating):
    """
    Combine wikidata with rotten tomatoes data to get specific rating
    :param wikidata_df: Dataframe, wikidata dataframe
    :param rating: String, type of rating to focus
    :return: dataframe, wikidata and rotten tomatoes data merged
    """
    rt_df = json_to_df('rotten-tomatoes')
    rt_df = rt_df[['rotten_tomatoes_id', rating]]
    rt_df = rt_df.dropna(subset=[rating])
    wikidata_df = pd.merge(wikidata_df, rt_df, on='rotten_tomatoes_id')
    return wikidata_df


def map_wikidata_id(data, category):
    """
    Map the wikidata id to the corresponding label
    :param data: Dataframe, dataframe with wikidata_id to be mapped
    :param category: String, column of interest
    :return: Dataframe with column mapped
    """
    category_df = json_to_df(category)
    mapping = category_df.set_index('wikidata_id').T.to_dict('records')[0]
    data[category] = data[category].map(mapping)
    return data


def split_list_into_rows(row, row_accumulator, column):
    """
    adapted from https://gist.github.com/jlln/338b4b0b55bd6984f883
    If a row in the dataframe has a list of values in 'column', split the row
    into multiple rows
    :param row: Series, row of dataframe to be split
    :param row_accumulator: list, new rows to be made into dataframe
    :param column: String, column of dataframe to split on
    """

    split_row = row[column]
    for s in split_row:
        new_row = row.to_dict()
        new_row[column] = s
        row_accumulator.append(new_row)


def explode_dataframe_by_column(data, column):
    """
    Adapted from https://gist.github.com/jlln/338b4b0b55bd6984f883
    Creates a new dataframe with the list in 'column' split into multiple rows
    :param data: Dataframe, wikidata
    :param column: String, column of interest
    :return: Dataframe with 'column' split
    """

    new_rows = []
    data = data.dropna(subset=[column])
    data.apply(split_list_into_rows, axis=1,
               args=(new_rows, column))
    new_df = pd.DataFrame(new_rows)
    return new_df


def avg_by_category(data, category, rating):
    """
    Keeps numerical values of interest around the 'category' and organizes the
    dataframe by the person of interest.
    :param data: Dataframe, wikidata with category of interest
    :param category: String, column of interest
    :param rating: String, type of rating to focus
    :return: Dataframe, organized by persons average scores and number of movies
    """
    new_df = explode_dataframe_by_column(data, category)
    req_columns = [category, rating]

    grouped_df = new_df[req_columns].groupby(category)
    avg_df = grouped_df.agg('mean')
    count = grouped_df[category].count()
    count_df = pd.DataFrame({category: count.index, 'movies': count.values})
    merged_df = pd.merge(avg_df, count_df, on=category)
    return merged_df


def get_top(data, rating, min_num_of_movies, num_of_influencers):
    """
    Remove any person/genre involved in less than (min num of movies) movies
    (arbitrary) and sort by the rating (return, rotten_tomatos scores).
    Select the top (num of influncers) values
    :param data: Dataframe,
    :param rating: String, type of rating to focus
    :param min_num_of_movies: int, filter out any with less than
    :param num_of_influencers: int, number of points to take
    :return: dataframe sorted by descending score,
    """
    data = data.loc[(data['movies'] >= min_num_of_movies)]
    data = data.sort_values(rating, ascending=False)
    data = data.head(num_of_influencers)
    return data


def get_best_rated(wikidata_df, category, rating,
                   min_num_of_movies,
                   num_of_influencers):
    """
    Find all the top people/genre of each category
    :param wikidata_df: Dataframe, wikidata
    :param rating: String, type of rating to focus
    :param min_num_of_movies: int, filter out any with less than
    :param num_of_influencers: int, number of points to take
    :return: list of best rated people/genres
    """
    data = avg_by_category(wikidata_df, category, rating)
    best_rated = get_top(data, rating, min_num_of_movies, num_of_influencers)
    return best_rated[category].tolist()


def filter_category(category, influencers):
    """
    Ignore values not in the list of influencers
    :param category: List, influencing points in a category
    :param influencers: List, most popular infleuncing points
    :return: List, only popular influencers
    """
    new_list = list(set(influencers) & set(category))
    if new_list:
        return new_list
    else:
        return np.nan


def get_filtered_wikidata(wikidata_file, category, rating,
                          min_num_of_movies=5,
                          num_of_influencers=50,
                          year=None):
    """
    Filter the movie database and select the category and
    :param wikidata_file: String, name of the wikidata file
    :param category: String, category of interest (genre, cast_member, director)
    :param rating: String, type of rating to focus
    :param min_num_of_movies: int, filter out any with less than
    :param num_of_influencers: int, number of popular categories to use
    :return: dataframe, movies by category
    """
    """
    Get the notable points (cast members, directors, genres) of a movie and the
    score in question (money return or rotten tomatoes score)
    :param wikidata_file: String, name of the wikidata movie file
    :param score: string, rating to focus on
    :return: Dataframe, filtered dataframe with all notable points
    """
    print("DEBUG: Start wikidata filter")
    wikidata_df = json_to_df(wikidata_file)
    wikidata_df = prep_wikidata(wikidata_df, category, rating, year)

    print("DEBUG: Get most influential ")
    influencers = get_best_rated(wikidata_df, category, rating,
                                 min_num_of_movies,
                                 num_of_influencers)
    wikidata_df[category] = wikidata_df[category].apply(
        filter_category,
        influencers=influencers)

    cleaned_wikidata_df = wikidata_df[['label', 'publication_date',
                                       category, rating]].dropna()

    return cleaned_wikidata_df


def main():
    # req_columns = [column, 'audience_average', 'audience_percent',
    #                'audience_ratings', 'critic_average', 'critic_percent',
    #                'nbox', 'ncost', 'return']
    wikidata_file = sys.argv[1]
    # category = 'genre'
    # rating = 'critic_percent'
    # wikidata_df = json_to_df(wikidata_file)
    # wikidata_df = prep_wikidata(wikidata_df, category, rating)
    #
    #
    #
    # influencers = get_best_rated(wikidata_df, category, rating,
    #                              min_num_of_movies=40,
    #                              num_of_influencers=50)
    # category_df = json_to_df(category)
    # mapping = category_df.set_index('wikidata_id').T.to_dict('records')[0]
    # influencers[category] = pd.Series(influencers[category]).map(mapping)

    data = get_filtered_wikidata(wikidata_file, 'genre', 'critic_percent',
                                 40, 50)

    print("Done!")

if __name__ == '__main__':
    main()
