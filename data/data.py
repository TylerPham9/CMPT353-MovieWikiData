import pandas as pd
import gzip


def get_genres():
    data = pd.read_json(gzip.open('genres.json.gz', 'rt',
                                  encoding='utf-8'), lines=True)
    print(list(data.columns.values))
    return data


def get_omdb_data():
    data = pd.read_json(gzip.open('omdb-data.json.gz', 'rt',
                                  encoding='utf-8'), lines=True)
    print(list(data.columns.values))
    return data


def get_rt_data():
    data = pd.read_json(gzip.open('rotten-tomatoes.json.gz', 'rt',
                                  encoding='utf-8'), lines=True)
    print(list(data.columns.values))
    return data


def get_wiki_data():
    data = pd.read_json(gzip.open('wikidata-movies.json.gz', 'rt',
                                  encoding='utf-8'), lines=True)
    print(list(data.columns.values))
    return data


def main():
    genres = get_genres()
    omdb_data = get_omdb_data()
    rt_data = get_rt_data()
    wiki_data = get_wiki_data()
    print(genres)


if __name__ == '__main__':
    main()
