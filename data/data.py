import sys
import pandas as pd
import numpy as np
import gzip


def json_to_dataframe(file):
    """
    opens the gzip file into a pd dataframe
    :param file: string, name of the file
    :return: dataframe of the json file
    """
    data = pd.read_json(gzip.open('json/{}.json.gz'.format(file), 'rt',
                                  encoding='utf-8'), lines=True)
    return data

#
# def replace_id(ids, id_mapping):
#     mapped = []
#     if isinstance(ids, list):
#         for id in ids:
#             if id in id_mapping:
#                 mapped += [id_mapping[id]]
#     if mapped:
#         return mapped
#     else:
#         return np.nan

def replace_id(id, id_mapping):
    if id in id_mapping:
        return id_mapping[id]
    else:
        return np.nan



def map_wikidata_id(wikidata_df, mapping_df, column):
    # for category in list(mapping_df.keys()):
    mapping = mapping_df[column].set_index(
        'wikidata_id').T.to_dict('records')[0]
    wikidata_df[column] = wikidata_df[column].apply(
        replace_id,
        id_mapping=mapping,
    )
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
                    (data['return'] <= 5000)]
    return data.sort_values('return', axis=0,
                            ascending=False)


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


def stats_by_column(data, column):
    """
    Keeps numerical values of interest around the 'column' and organizes the
    dataframe by the person of interest.
    :param data: Dataframe, wikidata with rotten tomatoe data
    :param column: String, column of interest
    :return: Dataframe, organized by persons average scores and number of movies
    """
    new_df = explode_dataframe_by_column(data, column)
    req_columns = [column, 'audience_average', 'audience_percent',
                   'audience_ratings', 'nbox', 'ncost', 'return']
    grouped_df = new_df[req_columns].groupby(column)
    avg_df = grouped_df.agg('mean')
    count = grouped_df[column].count()
    count_df = pd.DataFrame({column: count.index, 'movies': count.values})
    return pd.merge(avg_df, count_df, on=column)

def main():
    wiki_data_file = sys.argv[1]
    mapping_json_files = ['cast_member', 'director', 'genre']
    # json_files = [wiki_data_file]
    mapping_df = {}

    for file in mapping_json_files:
        mapping_df[file] = json_to_dataframe(file)

    wikidata_df = json_to_dataframe(wiki_data_file)
    omdb_data_df = json_to_dataframe('omdb-data')
    rotten_tomatoes_df = json_to_dataframe('rotten-tomatoes')

    wikidata_df = clean_return_data(wikidata_df)
    wikidata_df = pd.merge(wikidata_df, rotten_tomatoes_df,
                           on='rotten_tomatoes_id')


    wikidata_cast_member_df = stats_by_column(wikidata_df, 'cast_member')
    mapped = map_wikidata_id(wikidata_cast_member_df, mapping_df, 'cast_member')
    print(mapping_json_files)


if __name__ == '__main__':
    main()
