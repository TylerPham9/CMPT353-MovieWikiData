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
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument("--mode", '-m', type=str)
parser.add_argument("--category", "-c", type=str,
                    help="Property to focus on (cast_member, director, genre)",
                    choices=['genre', 'cast_member', 'director'])
parser.add_argument("--score", "-s", type=str,
                    help="Score to focus on (critic_percent, return)",
                    choices=['return', 'critic_percent', 'critic_average',
                             'audience_percent', 'audience_average'])
parser.add_argument("--movies", "-mo", type=int,
                    help="Minimum number of movies of each property",
                    default=40)
parser.add_argument("--influencers", "-i", type=int,
                    help="Number of influential people/genres",
                    default=25)
parser.add_argument("--year", "-y", type=int, nargs='+',
                    help="Bound the publication dates by the years",
                    default=None)

args = parser.parse_args()


filename = '{}-{}-{}-{}'.format(args.category, args.score, args.movies,
                                args.influencers)

if args.year:
    filename += "-{}-{}".format(args.year[0], args.year[1])

ALPHA = 0.05


def get_movie_data():
    """
    Based on the parameters, filter and clean the data and create a json file.
    If the json file is already created, use that instead.
    :return: dataframe, movie data
    """
    if os.path.isfile(dm.JSON_PATH.format(filename)):
        data = dm.json_to_df(filename)
    else:
        data = dm.get_filtered_wikidata('wikidata-movies', args.category,
                                        args.score, args.movies,
                                        args.influencers, args.year)
        data = dm.explode_dataframe_by_column(data, args.category)
        data = dm.map_wikidata_id(data, args.category)
        dm.df_to_json(data, filename)

    return data


def main():
    data = get_movie_data(filename)

    print("DEGUG: TukeyHSD")
    data_pivoted = data.pivot(columns=args.category)[args.score]
    new_data = [data_pivoted[col].dropna() for col in data_pivoted]
    anova = stats.f_oneway(*new_data)
    print("P Value from f_oneway of the {} {}s is {}".format(
        args.influencers,
        args.category.replace("_", " "),
        anova.pvalue))

    if anova.pvalue <= ALPHA:
        print("There is a difference between means, proceed to Tukey's HSD")
        posthoc = pairwise_tukeyhsd(
            data[args.score], data[args.category],
            alpha=ALPHA)

        title_score = args.score.title().replace("_", " ")
        if args.category == "return":
            xlabel = "Percent Return (box office/cost)"
        else:
            xlabel = "Rotten Tomatoe's {}".format(
                title_score)
        ylabel = args.category.title().replace("_", " ") + 's'

        title = "{}' {} Comparison".format(
            ylabel, title_score
        )

        fig = posthoc.plot_simultaneous(
            xlabel=xlabel,
            ylabel=ylabel,
            figsize=(16, 8))
        fig.suptitle(title)
        # fig.show()
        fig.savefig("figures/" + filename)
    else:
        print("Can't confirm there is a difference between means")

    print("Done!")

if __name__ == '__main__':
    main()
