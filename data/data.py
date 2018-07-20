import sys
import pandas as pd
import numpy as np
import gzip


def json_to_dataframe(file):
    data = pd.read_json(gzip.open('json/{}.json.gz'.format(file), 'rt',
                                  encoding='utf-8'), lines=True)
    return data


def replace_id(ids, id_mapping):
    mapped = []
    if ids is not None:
        for id in ids:
            if id in id_mapping:
                mapped += [id_mapping[id]]
    if mapped:
        return mapped
    else:
        return None


def map_wikidata_id(wikidata_df, mapping_df):
    for category in list(mapping_df.keys()):
        mapping = mapping_df[category].set_index(
            'wikidata_id').T.to_dict('records')[0]
        wikidata_df[category] = wikidata_df[category].apply(
            replace_id,
            id_mapping=mapping,
        )
        print(category)
    return wikidata_df


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

    # data = pd.read_json(gzip.open('json\wd-0236.json.gz', 'rt',
    #                               encoding='utf-8'), lines=True)
    # genres = get_genres()
    # omdb_data = get_omdb_data()
    # rt_data = get_rt_data()
    # wiki_data = get_wiki_data()
    # print(genres)
    # df[wiki_data_file].head(30).to_json('wikidata-movies-small.json.gz',
    #                                  orient='records',
    #                                  lines=True, compression='gzip')
    # wiki_data['genre'] = wiki_data['genre'].apply(replace_id_with_genre,
    #                                               list_of_genres=genres)

    # genre_mapping = df['genres'].set_index('wikidata_id').T.to_dict('records')[0]

    # df[wiki_data_file]['genre'] = df[wiki_data_file]['genre'].apply(
    #     replace_id,
    #     id_mapping=df['genres'],
    # )
    # categories = ['cast_member', 'director', 'executive_producer', 'genres']

    # mapping = mapping_df['cast_member'].set_index('wikidata_id').T.to_dict('records')[0]
    # wikidata_df['cast_member'] = wikidata_df['cast_member'].apply(
    #     replace_id,
    #     id_mapping=mapping,
    # )


    mapped_wikidata = map_wikidata_id(wikidata_df, mapping_df)
    print(mapping_json_files)


if __name__ == '__main__':
    main()
