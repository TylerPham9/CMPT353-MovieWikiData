import sys
import pandas as pd
import numpy as np
import gzip
from os import path

JSON_PATH = path.dirname(__file__) + '/json/{}.json.gz'
CATEGORIES = ['cast_member', 'director', 'genre']


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
    :return:
    """
    df.to_json(path.abspath(JSON_PATH.format(file)),
               orient='records',
               lines=True,
               compression='gzip')


def replace_ids(ids, id_mapping):
    """
    Given a list of ids, map each id to a proper name
    :param ids: list of strings, wikidata ids
    :param id_mapping: Dict, ids with their corresponding proper name
    :return: list of proper names
    """
    mapped = []
    if isinstance(ids, list):
        for id in ids:
            if id in id_mapping:
                mapped += [id_mapping[id]]
    if mapped:
        return mapped
    else:
        return np.nan


def map_wikidata_id(wikidata_df, category):
    """
    Map all the ids of the category in the wikidata dataframe
    :param wikidata_df: Dataframe, wikidata
    :param category: String, category to map
    :return: dataframe
    """
    mapping_df = json_to_df(category)
    mapping = mapping_df.set_index(
        'wikidata_id').T.to_dict('records')[0]

    # If the column contains a list, use the apply function
    if isinstance(wikidata_df[category].dropna().iloc[0], list):
        wikidata_df[category] = wikidata_df[category].apply(
            replace_ids,
            id_mapping=mapping
        )
    # Else, the values can be mapped
    else:
        wikidata_df[category] = wikidata_df[category].map(mapping)

    return wikidata_df


def clean_return_data(data):
    """
    removes all rows without values in "return" and values deemed too small
    or too large
    :param data: dataframe, dataframe to be clean
    :return: dataframe
    """
    data = data[pd.notnull(data['return'])]
    data = data.loc[(data['return'] >= 0.0001) &
                    (data['return'] <= 3000)]
    return data.sort_values('return', axis=0,
                            ascending=False)


def clean_rt_data(data):
    """
    Removes bad values from the Rotten Tomatoes dataframe
    :param data: dataframe, Rotten Tomatoes data
    :return: dataframe
    """
    data = data.dropna()
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


def stats_by_column(data, column, score):
    """
    Keeps numerical values of interest around the 'column' and organizes the
    dataframe by the person of interest.
    :param data: Dataframe, wikidata with rotten tomatoe data
    :param column: String, column of interest
    :return: Dataframe, organized by persons average scores and number of movies
    """
    new_df = explode_dataframe_by_column(data, column)
    req_columns = [column, score]

    grouped_df = new_df[req_columns].groupby(column)
    avg_df = grouped_df.agg('mean')
    count = grouped_df[column].count()
    count_df = pd.DataFrame({column: count.index, 'movies': count.values})
    merged_df = pd.merge(avg_df, count_df, on=column)
    new_columns = list(merged_df.columns.values)
    new_columns.insert(0, new_columns.pop(new_columns.index(column)))
    merged_df = merged_df[new_columns]
    return merged_df


def mapping_and_splitting(wikidata_df, category, score, make_json=False):
    """
    Creates a dataframe of average scores by the category. Can make a json.gzip
    file
    :param wiki_data_file: Dataframe, wikidata of movies
    :param category: String, category to focus on
    :param make_json: Bool, option to create a json file
    :return dataframe with wikidata ids mapped
    """

    wikidata_cm_df = stats_by_column(wikidata_df, category, score)
    mapped_cm = map_wikidata_id(wikidata_cm_df, category)
    if make_json:
        df_to_json(mapped_cm, 'avg_by_{}'.format(category))
    return mapped_cm


def get_top(data, score):
    """
    Remove any person involved in less than 5 movies (arbitrary) and sort by the
    score (return, rotten_tomatos scores). Select the top 50 values
    :param data: dataframe
    :param score: string
    :return: dataframe sorted by descending score,
    """
    data = data.loc[(data['movies'] >= 5)]
    data = data.sort_values(score, ascending=False)
    data = data.head(50)
    return data


def get_interesting_points(wikidata_df, score, make_file=False):
    """
    Find all the top points of each category
    :param wikidata_df: Dataframe, wikidata
    :param score: string, criteria to focus on
    :param make_file: bool, if true create a json file
    :return: dataframe of top points
    """
    top_categories = []

    for category in CATEGORIES:
        mapped_df = mapping_and_splitting(wikidata_df, category, score)
        cleaned_df = get_top(mapped_df, score)
        top_categories += cleaned_df[category].tolist()

    interesting_points = pd.DataFrame(top_categories, columns=['point'])
    if make_file:
        df_to_json(interesting_points, 'interesting_points_by_{}'.format(score))
    return interesting_points


def filter_and_join(row, interesting_points):
    """
    Combine all the catergories into one column and ignore points that are
    not interesting
    :param row: row of wikidat
    :param interesting_points: list of strings: interesting points
    :return:
    """
    new_list = []

    for category in CATEGORIES:
        # Ignore if column is nan
        if isinstance(row[category], list):
            for value in row[category]:
                # Filter out points
                if value in interesting_points:
                    new_list += [value]

    if new_list:
        return new_list
    else:
        return np.nan


def get_filtered_wikidata(wikidata_file, score):
    """
    Get the notable points (cast members, directors, genres) of a movie and the
    score in question (money return or rotten tomatoes score)
    :param wikidata_file: String, name of the wikidata movie file
    :param score: string, score to focus on
    :return: Dataframe, filtered dataframe with all notable points
    """
    wikidata_df = json_to_df(wikidata_file)

    if score == 'return':
        wikidata_df = clean_return_data(wikidata_df)
    else:
        rotten_tomatoes_df = json_to_df('rotten-tomatoes')
        rotten_tomatoes_df = clean_rt_data(rotten_tomatoes_df)

        wikidata_df = pd.merge(wikidata_df, rotten_tomatoes_df,
                               on='rotten_tomatoes_id')

    interesting_points = get_interesting_points(wikidata_df, score)
    # Map all the points
    for category in CATEGORIES:
        wikidata_df = map_wikidata_id(wikidata_df, category)
    # interesting_points = json_to_df('interesting_points')['point'].tolist()

    # poi = points of interest
    # Combine all the catergories into 1 list
    wikidata_df['poi'] = wikidata_df.apply(filter_and_join, axis=1,
                                           interesting_points=interesting_points)
    cleaned_wikidata_df = wikidata_df[['label', 'poi', score]].dropna()

    df_to_json(cleaned_wikidata_df, 'movies_by_{}'.format(score))
    return cleaned_wikidata_df


def main():
    # req_columns = [column, 'audience_average', 'audience_percent',
    #                'audience_ratings', 'critic_average', 'critic_percent',
    #                'nbox', 'ncost', 'return']
    wikidata_file = sys.argv[1]
    get_filtered_wikidata(wikidata_file, 'critic_average')


if __name__ == '__main__':
    main()
